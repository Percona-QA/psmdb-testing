import re
import time
from contextlib import ExitStack

import pymongo
import pytest
from cluster import Cluster
from data_integrity_check import compare_data

import docker

WC_SCOPES = ("clone", "repl")

def wc_values(log_text):
    """Map each stage to the write concern values it logged, in order"""
    values = {scope: [] for scope in WC_SCOPES}
    for line in log_text.splitlines():
        match = re.search(r"Config: TargetWriteConcern: (?P<value>\S+).*?\bs=(?P<scope>clone|repl)\b", line)
        if match:
            values[match.group("scope")].append(match.group("value"))
    return values

def assert_wc_logged(csync, expected, since=None, timeout=30):
    """Fail unless both clone and repl logged the expected write concern, and nothing else"""
    for scope in WC_SCOPES:
        assert csync.wait_for_log(f"Config: TargetWriteConcern: {expected} s={scope}", timeout=timeout,
                                  since=since), (
            f"no 'Config: TargetWriteConcern: {expected}' line for s={scope}: {csync.logs(tail=30, since=since)}")
    values = wc_values(csync.logs(tail=None, since=since))
    for scope in WC_SCOPES:
        assert set(values[scope]) == {expected}, (
            f"s={scope} logged write concern {values[scope]}, expected only {expected!r}")

def checkpoint_wc(dst_cluster):
    """
    (state, targetWriteConcern) from the saved checkpoint, or None if there is none yet.
    PCSM omits the field when the run uses majority, so a missing field reads as 'majority'.
    """
    client = pymongo.MongoClient(dst_cluster.connection)
    doc = client["percona_clustersync_mongodb"]["checkpoints"].find_one({"_id": "pcsm"})
    if not doc:
        return None
    data = doc.get("data", {})
    return data.get("state"), data.get("targetWriteConcern", "majority")

def wait_for_checkpoint_wc(dst_cluster, state, timeout=60):
    """Wait for a checkpoint in the given state and return its write concern"""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = checkpoint_wc(dst_cluster)
        if last and last[0] == state:
            return last[1]
        time.sleep(0.5)
    raise AssertionError(f"no '{state}' checkpoint within {timeout}s, last seen {last}")

def wait_for_state(csync, state, timeout=60):
    deadline = time.time() + timeout
    data = {}
    while time.time() < deadline:
        status = csync.status()
        data = status.get("data") or {}
        if data.get("state") == state:
            return data
        time.sleep(1)
    raise AssertionError(f"state never became '{state}' within {timeout}s, last status {data}")

def seed_source(src_cluster, count=100):
    """Small collection so the clone stage has data to insert"""
    client = pymongo.MongoClient(src_cluster.connection)
    client["wc_db"]["clone_coll"].insert_many([{"_id": i, "value": i} for i in range(count)])

def write_repl_data(src_cluster, start=1000, count=100):
    """Insert, update and delete on the source while replication is running"""
    coll = pymongo.MongoClient(src_cluster.connection)["wc_db"]["repl_coll"]
    coll.insert_many([{"_id": i, "value": i} for i in range(start, start + count)])
    coll.update_many({"_id": {"$lt": start + count // 2}}, {"$set": {"updated": True}})
    coll.delete_many({"_id": {"$gte": start + count - 10}})

def set_secondary_delay(cluster, delay_secs):
    """
    Make every secondary of a replica set apply the oplog delay_secs behind
    the primary
    """
    client = pymongo.MongoClient(cluster.connection)
    config = client.admin.command("replSetGetConfig")["config"]
    for member in config["members"][1:]:
        member["secondaryDelaySecs"] = delay_secs
        member["priority"] = 0 if delay_secs else 1
        member["hidden"] = bool(delay_secs)
    config["version"] += 1
    client.admin.command("replSetReconfig", config)
    Cluster.log(f"Set secondaryDelaySecs={delay_secs} on {cluster.config['_id']} secondaries")

def node_client(cluster: Cluster, host: str) -> pymongo.MongoClient:
    return pymongo.MongoClient(
        f"mongodb://root:root@{host}:27017/?replicaSet={cluster.config['_id']}&directConnection=true",
        serverSelectionTimeoutMS=5000, socketTimeoutMS=20000,
    )

def arm_failpoint(cleanup: ExitStack, client: pymongo.MongoClient, name: str) -> int:
    # Even if the enable command's response is lost, try to turn it off.
    cleanup.callback(client.admin.command, "configureFailPoint", name, mode="off")
    return client.admin.command("configureFailPoint", name, mode="alwaysOn")["count"] + 1

def wait_for_failpoint(client: pymongo.MongoClient, name: str, count: int) -> None:
    client.admin.command("waitForFailPoint", name, timesEntered=count, maxTimeMS=10000)

def extend_t139_election_timeouts(
    primary: pymongo.MongoClient, secondaries: list[pymongo.MongoClient],
) -> None:
    # This fixture stalls majority acknowledgments, not the target primary.
    config = primary.admin.command("replSetGetConfig")["config"]
    config["version"] += 1
    config["settings"]["electionTimeoutMillis"] = 60000
    config["settings"]["heartbeatTimeoutSecs"] = 60
    primary.admin.command("replSetReconfig", config, maxTimeMS=10000)

    deadline = time.monotonic() + 10
    while not all(secondary.admin.command("hello").get("setVersion") == config["version"]
                  for secondary in secondaries):
        assert time.monotonic() < deadline, "target secondaries did not install the test replica set config"
        time.sleep(0.2)

def has_pending_clone_ddl(client: pymongo.MongoClient) -> bool:
    # Clone can wait on the majority drop before it reaches the majority create.
    return bool(client.admin.command({
        "currentOp": 1,
        "active": True,
        "secs_running": {"$gte": 1},
        "command.$db": "wc_db",
        "command.writeConcern.w": "majority",
        "$or": [{"command.drop": "clone_coll"}, {"command.create": "clone_coll"}],
    })["inprog"])


@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.csync_env({"PCSM_RECOVERY_CHECKPOINT_INTERVAL": "1s"})
@pytest.mark.timeout(600, func_only=True)
def test_restart_keeps_target_write_concern_PML_T135(start_cluster, src_cluster, dst_cluster, csync):
    """
    Verify targetWriteConcern is kept if PCSM is restarted, and a new run
    started without one goes back to majority
    """
    seed_source(src_cluster)
    assert csync.start(mode="cli", raw_args=["--target-write-concern=1"]), (
        "failed to start with --target-write-concern=1")
    assert csync.wait_for_repl_stage(), "failed to reach the replication stage"
    assert_wc_logged(csync, "1")
    assert wait_for_checkpoint_wc(dst_cluster, "running") == "1", "checkpoint did not save targetWriteConcern=1"

    restarted_at = time.time()
    assert csync.restart(), "failed to restart csync"
    assert_wc_logged(csync, "1", since=restarted_at)

    write_repl_data(src_cluster)
    assert csync.wait_for_zero_lag(), "failed to catch up on replication"
    assert csync.finalize(), "failed to finalize"

    # A fresh run from the finalized state must not inherit the previous run's value
    new_run_at = time.time()
    assert csync.start(), "failed to start a new run"
    assert csync.wait_for_repl_stage(), "new run failed to reach the replication stage"
    assert_wc_logged(csync, "majority", since=new_run_at)
    assert wait_for_checkpoint_wc(dst_cluster, "running") == "majority", (
        "new run without a write concern did not go back to majority")


@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_ha_takeover_keeps_target_write_concern_PML_T136(start_ha_cluster, src_cluster, dst_cluster):
    """
    Verify targetWriteConcern is kept when HA failover occurs
    """
    group = start_ha_cluster
    seed_source(src_cluster)
    active = group.active()
    assert active.start(raw_args={"targetWriteConcern": "1"}), "failed to start with targetWriteConcern=1"
    assert active.wait_for_repl_stage(), "failed to reach the replication stage"
    assert_wc_logged(active, "1")
    assert wait_for_checkpoint_wc(dst_cluster, "running") == "1", "checkpoint did not save targetWriteConcern=1"

    killed = group.kill_active()
    group.wait_for_single_active()
    promoted = group.active()
    assert promoted.name != killed.name, "a different instance must take over"
    assert_wc_logged(promoted, "1", timeout=60)

    write_repl_data(src_cluster)
    assert promoted.wait_for_zero_lag(), "promoted instance failed to catch up"
    assert promoted.finalize(), "failed to finalize on the promoted instance"


@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.csync_env({"PCSM_TARGET_WRITE_CONCERN": "2"})
@pytest.mark.timeout(300, func_only=True)
def test_target_write_concern_above_member_count_PML_T137(start_cluster, src_cluster, dst_cluster, csync):
    """
    Verify a targetWriteConcern higher than the target's data-bearing node
    count is accepted at start, then fails the run with a clear error
    """
    seed_source(src_cluster)
    # pcsm start reads the env var from the container it runs in
    assert csync.start(mode="cli"), "failed to start with PCSM_TARGET_WRITE_CONCERN=2"
    status = wait_for_state(csync, "failed", timeout=120)
    error = status.get("error") or ""
    assert "UnsatisfiableWriteConcern" in error, f"run failed for an unexpected reason: {status}"
    Cluster.log(f"w:2 on a single-node target failed with: {error}")
    initial_sync = status.get("initialSync") or {}
    assert not initial_sync.get("completed"), f"clone reported completed despite failed writes: {status}"

@pytest.mark.parametrize("cluster_configs", ["replicaset_3n"], indirect=True)
@pytest.mark.parametrize("write_concern", ["majority", "1"])
@pytest.mark.timeout(600, func_only=True)
def test_lagging_target_secondaries_during_replication_PML_T138(start_cluster, src_cluster, dst_cluster, csync, write_concern):
    """
    Verify the replication stage uses targetWriteConcern.

    The target secondaries are delayed 2s so majority and 1 behave differently: with
    one operation per write, majority writes trickle in one every ~2s while
    targetWriteConcern=1 writes land straight away. Once the delay is
    removed both runs must finish with matching data.
    """
    delay_secs = 2
    docs = 20
    window_secs = 15
    coll_name = "stall"
    src = pymongo.MongoClient(src_cluster.connection)["wc_db"][coll_name]
    dst = pymongo.MongoClient(dst_cluster.connection)["wc_db"][coll_name]
    src.insert_one({"_id": "seed"})
    options = {
        "targetWriteConcern": write_concern,
        "replNumWorkers": 1,
        "replBulkOpsSize": 1,
        "replWorkerBulkQueueSize": 1,
    }
    assert csync.start(raw_args=options), f"failed to start with w:{write_concern}"
    assert csync.wait_for_repl_stage(timeout=180), "failed to reach the replication stage"
    assert_wc_logged(csync, write_concern)

    set_secondary_delay(dst_cluster, delay_secs)
    try:
        for i in range(docs):
            src.insert_one({"_id": i})
        deadline = time.time() + window_secs
        applied = dst.count_documents({}) - 1
        while time.time() < deadline and applied < docs:
            time.sleep(0.5)
            applied = dst.count_documents({}) - 1
        Cluster.log(f"w:{write_concern}: {applied}/{docs} docs on target after {window_secs}s "
                    f"with secondaries {delay_secs}s behind")
        if write_concern == "1":
            assert applied == docs, (
                f"targetWriteConcern=1 writes were held up by lagging secondaries: {applied}/{docs} in {window_secs}s")
        else:
            assert applied < docs, (
                f"majority writes were not slowed by lagging secondaries: {applied}/{docs} in {window_secs}s")
    finally:
        set_secondary_delay(dst_cluster, 0)

    assert csync.wait_for_zero_lag(), "failed to catch up once the secondaries recovered"
    assert csync.finalize(), "failed to finalize"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch: {mismatch}"

@pytest.mark.parametrize("cluster_configs", ["replicaset_3n"], indirect=True)
@pytest.mark.parametrize("write_concern", ["majority", "1"])
@pytest.mark.csync_env({"PCSM_HA_LEASE_TTL": "60s"})
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.timeout(600, func_only=True)
def test_majority_unreachable_during_clone_PML_T139(start_cluster, src_cluster, dst_cluster, csync, write_concern):
    """
    After a healthy start, clone must wait for majority target DDL even with
    w:1 data writes. The 60s lease must survive a 20s outage so the same run
    can finish once the secondaries resume.
    """
    outage_secs = 20
    seed_source(src_cluster)

    assert csync.wait_for_role("ACTIVE"), "csync server didn't come up"
    metrics = csync.metrics()["data"]
    transitions_before = metrics["percona_clustersync_mongodb_ha_role_transitions_total"]
    term_before = metrics["percona_clustersync_mongodb_ha_term"]

    with ExitStack() as target:
        primary = target.enter_context(node_client(dst_cluster, dst_cluster.primary_hosts[0]))
        assert primary.admin.command("hello")["isWritablePrimary"], "unexpected target primary"
        secondaries = [
            target.enter_context(node_client(dst_cluster, host))
            for host in dst_cluster.mongod_hosts[1:]
        ]
        for secondary in secondaries:
            assert secondary.admin.command("hello")["secondary"], "target node is not a secondary"
        extend_t139_election_timeouts(primary, secondaries)

        with ExitStack() as source:
            source_primary = source.enter_context(node_client(src_cluster, src_cluster.primary_hosts[0]))
            assert source_primary.admin.command("hello")["isWritablePrimary"], "unexpected source primary"
            source_gate = "hangInAppendOplogNote"
            source_count = arm_failpoint(source, source_primary, source_gate)
            assert csync.start(raw_args={"targetWriteConcern": write_concern}), (
                f"failed to start with w:{write_concern}: {csync.cmd_stdout} {csync.cmd_stderr}")
            assert_wc_logged(csync, write_concern)
            wait_for_failpoint(source_primary, source_gate, source_count)

            # The start checkpoint has committed. Hold clone at its source
            # anchor only until both target secondaries stop fetching the oplog.
            target_gate = "stopReplProducer"
            counts = [arm_failpoint(target, secondary, target_gate) for secondary in secondaries]
            for secondary, count in zip(secondaries, counts, strict=True):
                wait_for_failpoint(secondary, target_gate, count)

        # The source gate is off. Verify that a real clone DDL is still
        # pending on the target before timing the outage observation.
        deadline = time.monotonic() + 10
        while not has_pending_clone_ddl(primary):
            assert time.monotonic() < deadline, "no pending majority clone DDL on target"
            time.sleep(0.2)

        deadline = time.monotonic() + outage_secs
        while time.monotonic() < deadline:
            time.sleep(1)
            # Leave time for /status to return even if its HA lookup times out.
            status_code, data = csync.request("GET", "/status")
            assert status_code == 200 and isinstance(data, dict), (
                f"could not read ACTIVE status during the outage: HTTP {status_code}, {data!r}")
            assert data.get("role", "ACTIVE") == "ACTIVE", (
                f"csync stepped down before the raised lease TTL expired: {csync.logs(tail=30)}")
            assert data.get("state") == "running", f"run left the running state during the outage: {data}"
            assert (data.get("initialSync") or {}).get("cloneCompleted") is False, (
                f"clone completed while majority was unreachable: {data}")
        assert primary.admin.command("hello")["isWritablePrimary"], "target primary changed during the outage"
        assert has_pending_clone_ddl(primary), "majority clone DDL stopped waiting before target recovery"

    assert csync.wait_for_repl_stage(timeout=180), "run didn't continue once the secondaries caught up"
    metrics = csync.metrics()["data"]
    assert metrics["percona_clustersync_mongodb_ha_role_transitions_total"] == transitions_before, (
        "csync changed role during the outage despite the raised lease TTL")
    assert metrics["percona_clustersync_mongodb_ha_term"] == term_before, "lease term changed during the outage"

    write_repl_data(src_cluster)
    assert csync.wait_for_zero_lag(), "failed to catch up on replication"
    assert csync.finalize(), "failed to finalize"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch after majority was unreachable: {mismatch}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_reset_recovers_failed_clone_PML_T140(start_cluster, src_cluster, dst_cluster, csync):
    """
        Verify unsatisfiable targetWriteConcern recovery
    """
    seed_source(src_cluster)
    assert csync.start(raw_args={"targetWriteConcern": "2"}), "failed to start with targetWriteConcern=2"
    wait_for_state(csync, "failed", timeout=120)

    # A failed clone counts as finished, so a fresh start is refused until the state is reset
    assert not csync.start(mode="cli", raw_args=["--target-write-concern=1"]), "fresh start after a failed clone"
    assert "already running" in csync.cmd_stdout + csync.cmd_stderr, (
        f"fresh start refused for an unexpected reason: {csync.cmd_stdout} {csync.cmd_stderr}")

    # pcsm reset must run while no server is using the target
    csync.container.stop()
    output = docker.from_env().containers.run(
        image=csync.csync_image, network="test", remove=True,
        command=f"pcsm reset --target {csync.dst}", stderr=True).decode()
    assert "OK: reset all" in output, f"pcsm reset failed: {output}"
    assert csync.start_container(), "csync didn't come back after the reset"
    assert csync.wait_for_role("ACTIVE"), "csync not ACTIVE after the reset"
    wait_for_state(csync, "idle")

    new_run_at = time.time()
    assert csync.start(mode="cli", raw_args=["--target-write-concern=1"]), "failed to start a new run after the reset"
    assert csync.wait_for_repl_stage(), "new run failed to reach the replication stage"
    assert_wc_logged(csync, "1", since=new_run_at)
    write_repl_data(src_cluster)
    assert csync.wait_for_zero_lag(), "failed to catch up on replication"
    assert csync.finalize(), "failed to finalize"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch after recovering with pcsm reset: {mismatch}"
