import re
import time

import pymongo
import pytest
from cluster import Cluster
from data_integrity_check import compare_data

# PCSM-311: clone inserts and replication data writes use the run's target
# write concern, everything else (checkpoints, HA state, catalog DDL) stays
# majority. The effective value is logged once per stage when a run starts
# or recovers, e.g. "Config: TargetWriteConcern: 1 s=clone".
WC_LOG = re.compile(r"Config: TargetWriteConcern: (?P<value>\S+).*?\bs=(?P<scope>clone|repl)\b")
WC_SCOPES = ("clone", "repl")
INVALID_WC_ERROR = "invalid target write concern"
UNKNOWN_WC_FLAG = "unknown flag: --target-write-concern"
PCSM_DB = "percona_clustersync_mongodb"
TEST_DB = "wc_db"


def logs_since(csync, since=None):
    """Full csync container log, optionally only lines written since a unix timestamp"""
    kwargs = {"since": since} if since else {}
    return csync.container.logs(**kwargs).decode("utf-8", errors="replace")

def wc_values(log_text):
    """Map each stage to the write concern values it logged, in order"""
    values = {scope: [] for scope in WC_SCOPES}
    for line in log_text.splitlines():
        match = WC_LOG.search(line)
        if match:
            values[match.group("scope")].append(match.group("value"))
    return values

def wait_for_wc_logs(csync, since=None, timeout=30):
    """Poll until both clone and repl have logged their write concern"""
    deadline = time.time() + timeout
    values = wc_values(logs_since(csync, since))
    while time.time() < deadline and not all(values[scope] for scope in WC_SCOPES):
        time.sleep(0.5)
        values = wc_values(logs_since(csync, since))
    return values

def assert_wc_logged(csync, expected, since=None, timeout=30):
    values = wait_for_wc_logs(csync, since, timeout)
    for scope in WC_SCOPES:
        assert values[scope], (
            f"no 'Config: TargetWriteConcern' line for s={scope}: {logs_since(csync, since)[-3000:]}")
        assert set(values[scope]) == {expected}, (
            f"s={scope} logged write concern {values[scope]}, expected only {expected!r}")

def checkpoint_wc(dst_cluster):
    """
    (state, targetWriteConcern) from the saved checkpoint, or None if there is none yet.
    PCSM omits the field when the run uses majority, so a missing field reads as 'majority'.
    """
    client = pymongo.MongoClient(dst_cluster.connection)
    doc = client[PCSM_DB]["checkpoints"].find_one({"_id": "pcsm"})
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
    client[TEST_DB]["clone_coll"].insert_many([{"_id": i, "value": i} for i in range(count)])

def write_repl_data(src_cluster, start=1000, count=100):
    """Insert, update and delete on the source while replication is running"""
    coll = pymongo.MongoClient(src_cluster.connection)[TEST_DB]["repl_coll"]
    coll.insert_many([{"_id": i, "value": i} for i in range(start, start + count)])
    coll.update_many({"_id": {"$lt": start + count // 2}}, {"$set": {"updated": True}})
    coll.delete_many({"_id": {"$gte": start + count - 10}})

def drop_test_db(*clusters):
    for cluster in clusters:
        pymongo.MongoClient(cluster.connection).drop_database(TEST_DB)

def set_secondary_delay(cluster, delay_secs):
    """
    Make every secondary of a replica set apply the oplog delay_secs behind
    the primary. Delayed members must be priority 0, they're hidden too as
    MongoDB recommends. They still vote, so a majority write has to wait for
    one of them, which is how a lagging target is simulated. 0 restores them.
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


@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(900, func_only=True)
def test_target_write_concern_start_paths_PML_T135(start_cluster, src_cluster, dst_cluster, csync):
    """
    PCSM-311: every way of starting a run applies the expected write concern
    and logs it for both clone and repl, while invalid values are rejected
    before a run starts. Covers the env var being read only by 'pcsm start'
    (the server ignores it) and the API rejecting a JSON number.
    """
    server_env = {"PCSM_TARGET_WRITE_CONCERN": "1"}
    # (description, container env, extra server args, mode, start args, should_start, expected output, expected wc)
    test_cases = [
        ("API default", {}, "", "http", {}, True, '"ok":true', "majority"),
        ("API w:1", {}, "", "http", {"targetWriteConcern": "1"}, True, '"ok":true', "1"),
        ("API majority", {}, "", "http", {"targetWriteConcern": "majority"}, True, '"ok":true', "majority"),
        ("CLI default", {}, "", "cli", [], True, '"ok": true', "majority"),
        ("CLI flag w:1", {}, "", "cli", ["--target-write-concern=1"], True, '"ok": true', "1"),
        ("CLI env w:1", server_env, "", "cli", [], True, '"ok": true', "1"),
        ("CLI flag beats env", server_env, "", "cli", ["--target-write-concern=majority"], True,
         '"ok": true', "majority"),
        ("API ignores server env", server_env, "", "http", {}, True, '"ok":true', "majority"),
        ("auto-start ignores server env", server_env, "--start", None, None, True, None, "majority"),
        ("API rejects 0", {}, "", "http", {"targetWriteConcern": "0"}, False, INVALID_WC_ERROR, None),
        ("API rejects name", {}, "", "http", {"targetWriteConcern": "acknowledged"}, False,
         INVALID_WC_ERROR, None),
        ("API rejects JSON number", {}, "", "http", {"targetWriteConcern": 1}, False, "Bad Request", None),
        ("CLI rejects 0", {}, "", "cli", ["--target-write-concern=0"], False, INVALID_WC_ERROR, None),
        ("CLI rejects -1", {}, "", "cli", ["--target-write-concern=-1"], False, INVALID_WC_ERROR, None),
        ("CLI rejects env 0", {"PCSM_TARGET_WRITE_CONCERN": "0"}, "", "cli", [], False, INVALID_WC_ERROR, None),
    ]
    seed_source(src_cluster)
    failures = []
    for idx, (desc, env, extra_args, mode, raw_args, should_start, expected_output, expected_wc) in \
            enumerate(test_cases):
        try:
            drop_test_db(dst_cluster)
            csync.create(extra_args=f"--reset-state {extra_args}".strip(), env_vars=env)
            if mode is not None:
                result = csync.start(mode=mode, raw_args=raw_args)
                assert result == should_start, f"expected start()={should_start}, got {result}"
                output = csync.cmd_stdout + csync.cmd_stderr
                assert expected_output in output, (
                    f"expected {expected_output!r} in output, got stdout={csync.cmd_stdout!r} "
                    f"stderr={csync.cmd_stderr!r}")
            if should_start:
                assert csync.wait_for_repl_stage(), "failed to reach the replication stage"
                assert_wc_logged(csync, expected_wc)
            else:
                # Give a wrongly accepted run a moment to log before checking it didn't
                time.sleep(3)
                values = wc_values(logs_since(csync))
                assert not any(values.values()), f"a rejected start still started a run: {values}"
        except AssertionError as e:
            failures.append(f"Case {idx + 1} [{desc}]: {e!s}")
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} cases:\n" + "\n".join(failures))


@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.csync_env({"PCSM_RECOVERY_CHECKPOINT_INTERVAL": "1s"})
@pytest.mark.timeout(600, func_only=True)
def test_resume_cannot_change_target_write_concern_PML_T136(start_cluster, src_cluster, dst_cluster, csync):
    """
    PCSM-311: 'pcsm resume' rejects --target-write-concern as an unknown flag,
    and /resume ignores a targetWriteConcern field. The run keeps the value it
    was started with, both in the checkpoint and after a restart.
    """
    seed_source(src_cluster)
    assert csync.start(raw_args={"targetWriteConcern": "1"}), "failed to start with w:1"
    assert csync.wait_for_repl_stage(), "failed to reach the replication stage"
    assert_wc_logged(csync, "1")
    assert csync.pause(), "failed to pause"

    exit_code, stdout, stderr = csync.cli("resume --target-write-concern=2")
    assert exit_code != 0, f"'pcsm resume --target-write-concern=2' succeeded: {stdout} {stderr}"
    assert UNKNOWN_WC_FLAG in stdout + stderr, f"unexpected output: stdout={stdout!r} stderr={stderr!r}"
    assert wait_for_state(csync, "paused", timeout=10), "rejected CLI resume changed the state"

    resumed_at = int(time.time())
    code, body = csync.request("POST", "/resume", {"targetWriteConcern": "2"})
    assert code == 200 and body.get("ok") is True, f"/resume with an extra field failed: {code}, {body}"
    assert wait_for_checkpoint_wc(dst_cluster, "running") == "1", (
        "/resume changed the run's write concern in the checkpoint")
    values = wc_values(logs_since(csync, resumed_at))
    assert all(v == "1" for scope in WC_SCOPES for v in values[scope]), (
        f"write concern changed after /resume: {values}")

    restarted_at = int(time.time())
    assert csync.restart(), "failed to restart csync"
    assert_wc_logged(csync, "1", since=restarted_at)

    write_repl_data(src_cluster)
    assert csync.wait_for_zero_lag(), "failed to catch up on replication"
    assert csync.finalize(), "failed to finalize"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch: {mismatch}"


@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.csync_env({"PCSM_RECOVERY_CHECKPOINT_INTERVAL": "1s"})
@pytest.mark.timeout(600, func_only=True)
def test_restart_keeps_target_write_concern_PML_T137(start_cluster, src_cluster, dst_cluster, csync):
    """
    PCSM-311: the write concern belongs to the run. A restart mid-replication
    recovers the saved value, and a new run started without one goes back to
    majority.
    """
    seed_source(src_cluster)
    assert csync.start(raw_args={"targetWriteConcern": "1"}), "failed to start with w:1"
    assert csync.wait_for_repl_stage(), "failed to reach the replication stage"
    assert_wc_logged(csync, "1")
    assert wait_for_checkpoint_wc(dst_cluster, "running") == "1", "checkpoint did not save w:1"

    restarted_at = int(time.time())
    assert csync.restart(), "failed to restart csync"
    assert_wc_logged(csync, "1", since=restarted_at)

    write_repl_data(src_cluster)
    assert csync.wait_for_zero_lag(), "failed to catch up on replication"
    assert csync.finalize(), "failed to finalize"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch after restart with w:1: {mismatch}"

    # A fresh run from the finalized state must not inherit the previous run's value
    new_run_at = int(time.time())
    assert csync.start(), "failed to start a new run"
    assert csync.wait_for_repl_stage(), "new run failed to reach the replication stage"
    assert_wc_logged(csync, "majority", since=new_run_at)
    assert wait_for_checkpoint_wc(dst_cluster, "running") == "majority", (
        "new run without a write concern did not go back to majority")


@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_ha_takeover_keeps_target_write_concern_PML_T138(start_ha_cluster, src_cluster, dst_cluster):
    """
    PCSM-311: when the ACTIVE instance dies, the promoted instance recovers
    the run's write concern from the checkpoint instead of using majority.
    """
    group = start_ha_cluster
    seed_source(src_cluster)
    active = group.active()
    assert active.start(raw_args={"targetWriteConcern": "1"}), "failed to start with w:1"
    assert active.wait_for_repl_stage(), "failed to reach the replication stage"
    assert_wc_logged(active, "1")
    assert wait_for_checkpoint_wc(dst_cluster, "running") == "1", "checkpoint did not save w:1"

    killed = group.kill_active()
    group.wait_for_single_active()
    promoted = group.active()
    assert promoted.name != killed.name, "a different instance must take over"
    # The promoted instance never started a run itself, so any write concern
    # it logged came from recovering the checkpoint.
    assert_wc_logged(promoted, "1", timeout=60)

    write_repl_data(src_cluster)
    assert promoted.wait_for_zero_lag(), "promoted instance failed to catch up"
    assert promoted.finalize(), "failed to finalize on the promoted instance"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch after HA takeover with w:1: {mismatch}"


@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_target_write_concern_above_member_count_PML_T139(start_cluster, src_cluster, dst_cluster, csync):
    """
    PCSM-311: PCSM doesn't check the write concern against the target's size,
    so w:2 on single-node target replica sets is accepted at start. The run
    can never satisfy it, so it must fail with a clear error rather than
    hang or report a completed clone.
    """
    seed_source(src_cluster)
    started = csync.start(raw_args={"targetWriteConcern": "2"})
    if not started:
        # Rejecting it up front would also be acceptable
        assert INVALID_WC_ERROR in csync.cmd_stdout + csync.cmd_stderr, (
            f"start failed for an unexpected reason: {csync.cmd_stdout} {csync.cmd_stderr}")
        return
    status = wait_for_state(csync, "failed", timeout=120)
    error = status.get("error") or ""
    assert error.strip(), f"run failed with an empty error: {status}"
    Cluster.log(f"w:2 on a single-node target failed with: {error}")
    initial_sync = status.get("initialSync") or {}
    assert not initial_sync.get("completed"), f"clone reported completed despite failed writes: {status}"


@pytest.mark.jenkins
@pytest.mark.parametrize("cluster_configs", ["replicaset_3n"], indirect=True)
@pytest.mark.parametrize("write_concern", ["majority", "1"])
@pytest.mark.timeout(600, func_only=True)
def test_lagging_target_secondaries_PML_T140(start_cluster, src_cluster, dst_cluster, csync, write_concern):
    """
    PCSM-311: reproduce the original stall. Both target secondaries apply the
    oplog 2s behind the primary, so every majority write waits about 2s. With
    one replication worker writing one operation per bulk, majority writes
    trickle onto the target one every ~2s, while w:1 writes land straight
    away. Once the delay is removed both runs must finish with matching data.
    """
    delay_secs = 2
    docs = 20
    window_secs = 15
    coll_name = "stall"
    src = pymongo.MongoClient(src_cluster.connection)[TEST_DB][coll_name]
    dst = pymongo.MongoClient(dst_cluster.connection)[TEST_DB][coll_name]
    src.insert_one({"_id": "seed"})
    set_secondary_delay(dst_cluster, delay_secs)
    try:
        options = {
            "targetWriteConcern": write_concern,
            "replNumWorkers": 1,
            "replBulkOpsSize": 1,
            "replWorkerBulkQueueSize": 1,
        }
        assert csync.start(raw_args=options), f"failed to start with w:{write_concern}"
        assert csync.wait_for_repl_stage(timeout=180), "failed to reach the replication stage"
        assert_wc_logged(csync, write_concern)

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
                f"w:1 writes were held up by lagging secondaries: {applied}/{docs} in {window_secs}s")
        else:
            assert applied < docs, (
                f"majority writes were not slowed by lagging secondaries: {applied}/{docs} in {window_secs}s")
    finally:
        set_secondary_delay(dst_cluster, 0)

    assert csync.wait_for_zero_lag(), "failed to catch up once the secondaries recovered"
    assert csync.finalize(), "failed to finalize"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch: {mismatch}"
