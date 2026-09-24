import threading
import time
from datetime import datetime, timezone

import pymongo
import pytest
from cluster import Cluster
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data
from ha import GROUP_NAME

# The writer counts up from 1000, so FINAL_VALUE is one it can never write.
# Otherwise a stale overwrite would look the same as a correct one.
COUNTER_START = 1000
FINAL_VALUE = 10

def _status(inst):
    """The /status body, or {} if the instance did not answer."""
    return inst.status().get("data") or {}

def _wait_state(inst, state, timeout=60):
    """Wait for a state and return the status body, or None on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = _status(inst)
        if body.get("state") == state:
            return body
        time.sleep(0.5)
    return None

def _roster(inst):
    """The group members the instance currently sees as live."""
    return _status(inst).get("group", {}).get("members") or []

def _state_coll(dst_cluster, name):
    """One of PCSM's own collections on the target."""
    return pymongo.MongoClient(dst_cluster.connection)["percona_clustersync_mongodb"][name]

def _lease_doc(dst_cluster):
    return _state_coll(dst_cluster, "lease").find_one({"_id": "lease"})

class MetricsProbe:
    """
    Polls /metrics and records every failed scrape.
    """
    def __init__(self, instances, interval=0.3):
        self.instances = instances
        self.interval = interval
        self.failures = []
        self._stop = threading.Event()
        self._thread = None

    def __enter__(self):
        self._thread = threading.Thread(target=self._scrape, daemon=True)
        self._thread.start()
        return self

    def _scrape(self):
        while not self._stop.is_set():
            for inst in self.instances:
                if not inst.is_alive:
                    continue
                code, body = inst.request("GET", "/metrics", timeout=5)
                if code != 200:
                    self.failures.append((inst.name, code, str(body)[:120]))
            time.sleep(self.interval)

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

class CounterWriter:
    """
    Rewrites the same 50 documents on the source over and over.
    Reusing one small set of documents is what makes the check work: old
    ACTIVE is always holding an older copy of a document the test knows
    expected value of, so a late write from it is visible.
    """

    def __init__(self, src_cluster):
        self.client = pymongo.MongoClient(src_cluster.connection)
        self.coll = self.client["ha_counter_db"]["counter"]
        self.rounds = 0
        self._stop = threading.Event()
        self._thread = None

    def seed(self):
        self.coll.delete_many({})
        self.coll.insert_many([{"_id": i, "value": COUNTER_START} for i in range(50)])

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.coll.update_many({}, {"$set": {"value": COUNTER_START + self.rounds + 1}})
            except pymongo.errors.PyMongoError as e:
                Cluster.log(f"Counter writer stopped early: {e}")
                return
            self.rounds += 1

    def stop(self):
        """Safe to call twice - the test stops it, then teardown does too."""
        if self._stop.is_set():
            return
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=15)
        Cluster.log(f"Counter writer wrote {self.rounds} rounds")

    def write_final_value(self):
        self.coll.update_many({}, {"$set": {"value": FINAL_VALUE}})

    def close(self):
        self.stop()
        self.client.close()

def _seed_index_heavy_db(src_cluster):
    """
    Seed data that takes a while to finalize, so the kill can land mid-way.
    """
    db = pymongo.MongoClient(src_cluster.connection)["ha_finalize_db"]
    now = datetime.now(timezone.utc)
    for i in range(200):
        coll = db[f"index_collection_{i}"]
        coll.insert_many(
            [
                {"created": now, **{f"f{f}": f"c{i}-n{n}-f{f}" for f in range(4)}}
                for n in range(10)
            ],
            ordered=False)
        for f in range(4):
            coll.create_index([(f"f{f}", 1)], unique=True)
        coll.create_index([("created", 1)], expireAfterSeconds=100000)
    Cluster.log("Seeded 200 collections with 1000 deferred indexes")

def _drive_to_state(group, src_cluster, dst_cluster, state, operation_threads):
    """Put the pipeline into a given state and return the ACTIVE holding it."""
    active = group.active()
    if state == "idle":
        # Seed the source but never start: the promoted instance has to be
        # able to begin the run itself. Without data on the source there is
        # nothing for a later /start to clone.
        _, threads = create_all_types_db(
            src_cluster.connection, "init_test_db",
            start_crud=True, is_sharded=src_cluster.is_sharded)
        operation_threads += threads
        return active
    if state in ("cloning", "failed"):
        generate_dummy_data(
            src_cluster.connection, "ha_state_db",
            num_collections=10, doc_size=200000, is_sharded=src_cluster.is_sharded)
        assert active.start(raw_args={
            "cloneNumParallelCollections": 1,
            "cloneNumReadWorkers": 1,
            "cloneNumInsertWorkers": 1,
        }), "Failed to start throttled clone on ACTIVE"
        deadline = time.time() + 60
        while (_status(active).get("initialSync") or {}).get("clonedSizeBytes", 0) == 0:
            assert time.time() < deadline, "ACTIVE never reached a mid-clone state"
            time.sleep(0.2)
        assert not (_status(active).get("initialSync") or {}).get("cloneCompleted"), (
            "clone completed before the kill landed; increase the seed size")
        if state == "cloning":
            # A promoted instance can only restore a mid-clone state if one
            # was written, so wait for a checkpoint with an unfinished clone.
            checkpoints = _state_coll(dst_cluster, "checkpoints")
            unfinished = {
                "_id": "pcsm",
                "data.clone.startTime": {"$exists": True},
                "data.clone.finishTime": {"$exists": False},
            }
            deadline = time.time() + 45
            while not checkpoints.find_one(unfinished):
                assert time.time() < deadline, "the unfinished clone was never checkpointed"
                time.sleep(0.2)
            return active
        # 'failed' comes from that same mid-clone kill - an interrupted clone
        # is not resumable, so it is the one reliable way in.
        group.kill_active()
        group.wait_for_single_active()
        active = group.active()
        assert _wait_state(active, "failed"), "an interrupted clone must land in 'failed'"
        return active
    if state == "finalizing":
        _seed_index_heavy_db(src_cluster)
    _, threads = create_all_types_db(
        src_cluster.connection, "init_test_db",
        start_crud=True, is_sharded=src_cluster.is_sharded)
    operation_threads += threads
    assert active.start(), "Failed to start csync on ACTIVE"
    assert active.wait_for_repl_stage(timeout=300), "Failed to reach replication stage"
    if state == "paused":
        assert active.pause(), "Failed to pause on ACTIVE"
        assert _wait_state(active, "paused"), "ACTIVE did not settle into 'paused'"
    elif state == "finalizing":
        stop_all_crud_operations()
        assert active.wait_for_zero_lag(), "Failed to catch up before finalize"
        # Do not wait: finalization must still be running at the kill.
        active.request("POST", "/finalize", {})
    elif state == "finalized":
        stop_all_crud_operations()
        assert active.wait_for_zero_lag(), "Failed to catch up before finalize"
        assert active.finalize(), "Failed to finalize on ACTIVE"
    return active

@pytest.mark.ha_instances(5)
@pytest.mark.csync_env({"PCSM_RECOVERY_CHECKPOINT_INTERVAL": "15s"})
@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(1200, func_only=True)
def test_ha_single_active_invariant_PML_T127(start_ha_cluster, src_cluster, dst_cluster):
    """
    Exactly one ACTIVE, through a contested election and a crash failover.
    The kill lands mid-replication behind a backlog, so the promoted instance
    must replay it. Then the killed instance rejoins as STANDBY and after the
    whole group is killed a single restarted instance wins on its own.
    """
    group = start_ha_cluster
    assert len(group.instances) == 5
    assert len(group.standbys()) == 4, f"expected 4 STANDBY, got {[s.name for s in group.standbys()]}"
    lease = _lease_doc(dst_cluster)
    assert lease, "lease document missing on target"
    assert lease.get("term") == 1, f"a contested cold start must settle on term 1: {lease}"
    active = group.active()
    body = _status(active)
    assert body.get("me", {}).get("instanceId") == lease.get("instanceId"), (
        f"lease.instanceId {lease.get('instanceId')} != ACTIVE me {body.get('me')}")
    operation_threads = []
    try:
        _, threads = create_all_types_db(
            src_cluster.connection, "init_test_db",
            start_crud=True, is_sharded=src_cluster.is_sharded)
        operation_threads += threads
        assert active.start(), "Failed to start csync on ACTIVE"
        assert active.wait_for_repl_stage(), "Failed to reach replication stage"
        # Standbys must never touch the checkpoint - it is the ACTIVE's alone.
        assert active.wait_for_checkpoint(), "the finished clone was never checkpointed"
        checkpoint = _state_coll(dst_cluster, "checkpoints").find_one({"_id": "pcsm"})
        assert checkpoint.get("instanceId") == lease.get("instanceId"), (
            f"checkpoint written by a non-ACTIVE instance: {checkpoint.get('instanceId')}")
        _, threads = create_all_types_db(
            src_cluster.connection, "repl_test_db",
            start_crud=True, is_sharded=src_cluster.is_sharded)
        operation_threads += threads
        # A burst the ACTIVE cannot have drained, so the promoted instance
        # inherits a real backlog.
        generate_dummy_data(
            src_cluster.connection, "ha_backlog_db",
            num_collections=3, doc_size=40000, is_sharded=src_cluster.is_sharded)
        killed = group.kill_active()
        killed_at = time.time()
        Cluster.log(f"Killed ACTIVE '{killed.name}' during replication")
        group.wait_for_single_active()
        promoted = group.active()
        assert promoted.is_alive
        assert promoted.name != killed.name, "a different instance must take over after SIGKILL"
        assert len(group.standbys()) == 3
        lease = _lease_doc(dst_cluster)
        assert lease.get("term") == 2, f"takeover must bump the term exactly once: {lease}"
        body = _status(promoted)
        assert body.get("me", {}).get("instanceId") == lease.get("instanceId"), (
            f"lease.instanceId {lease.get('instanceId')} != promoted me {body.get('me')}")
        # LeaseTTL is 10s, so a takeover plus recovery should land inside 60s.
        assert _wait_state(promoted, "running", timeout=60), (
            f"promoted ACTIVE is {_status(promoted).get('state')}, not running")
        rto = time.time() - killed_at
        Cluster.log(f"Promoted ACTIVE '{promoted.name}' is running after {rto:.1f}s")
        assert rto < 60, f"failover took {rto:.1f}s, over the 60s budget"
        # A killed instance never deletes its own member doc, so the roster
        # has to drop it by heartbeat age. Check the roster, not the collection.
        deadline = time.time() + 30
        while len(_roster(promoted)) != 4:
            assert time.time() < deadline, (
                f"the dead instance stayed in the roster: {_roster(promoted)}")
            time.sleep(1)
        assert _state_coll(dst_cluster, "members").count_documents({}) == 5, (
            "a crashed instance is expected to leave its member doc behind")
        # It comes back as a plain STANDBY and must not disturb the run.
        assert killed.start_container(), f"'{killed.name}' did not come back up"
        assert killed.wait_for_role("STANDBY", timeout=60), "a returning instance must rejoin as STANDBY"
        assert group.active().name == promoted.name, "a returning instance must not steal the lease"
        deadline = time.time() + 30
        while len(_roster(promoted)) != 5:
            assert time.time() < deadline, (
                f"the returning instance did not rejoin the roster: {_roster(promoted)}")
            time.sleep(1)
        _, threads = create_all_types_db(
            src_cluster.connection, "repl_after_failover_db",
            start_crud=True, is_sharded=src_cluster.is_sharded)
        operation_threads += threads
    finally:
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()
    promoted = group.active()
    assert promoted.wait_for_zero_lag(), "Failed to catch up after failover"
    assert promoted.finalize(), "Failed to finalize after failover"
    # Re-applying events the old ACTIVE already wrote must be a no-op.
    assert "E11000" not in promoted.logs(tail=None), (
        "duplicate key errors while replaying after failover")
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch after failover during replication: {mismatch}"
    # Kill everything, then bring one back: it must win on its own.
    for inst in group.alive_instances():
        inst.kill()
    survivor = group.instances[0]
    assert survivor.start_container(), f"'{survivor.name}' did not come back up"
    assert survivor.wait_for_role("ACTIVE", timeout=60), "the only live instance must become ACTIVE"
    lease = _lease_doc(dst_cluster)
    assert lease.get("term") > 2, f"a fresh takeover must bump the term again: {lease}"

@pytest.mark.ha_instances(1)
@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_ha_standby_api_PML_T128(start_ha_cluster, dst_cluster):
    """
    STANDBY API and the group fields, across membership changes.
    Membership goes 1 -> 3 -> 1, so 'me', 'role' and 'group' are seen absent,
    present and absent again. /metrics is polled across a kill because it is
    the documented liveness probe - a stall there restarts a pod mid-failover.
    """
    group = start_ha_cluster
    solo = group.instances[0]
    body = _status(solo)
    assert body.get("state"), f"a lone instance must serve /status: {body}"
    for field in ("me", "role", "group"):
        assert field not in body, f"a lone instance must not publish '{field}': {body}"
    group.add_instance()
    group.add_instance()
    group.wait_for_single_active()
    for inst in group.instances:
        deadline = time.time() + 15
        while not _roster(inst):
            assert time.time() < deadline, (
                f"'{inst.name}' did not publish the group fields: {_status(inst)}")
            time.sleep(0.3)
        body = _status(inst)
        assert body.get("role") in ("ACTIVE", "STANDBY"), body
        assert len(body["group"]["members"]) == 3, body
    active = group.active()
    standby = group.standbys()[0]
    lease = _lease_doc(dst_cluster)
    active_addr = next(
        f"{m['host']}:{m['port']}" for m in _roster(active) if m.get("role") == "ACTIVE")
    for method, path, payload in (
        ("GET", "/status", None),
        ("POST", "/start", {}),
        ("POST", "/pause", None),
        ("POST", "/resume", None),
        ("POST", "/finalize", {}),
    ):
        code, body = standby.request(method, path, payload)
        assert code == 409, f"{method} {path}: expected 409, got {code} {body}"
        assert isinstance(body, dict), f"{method} {path}: expected JSON body, got {body!r}"
        assert body.get("error") == "not_active", body
        assert body.get("ok") is False, body
        assert body.get("role") == "STANDBY", body
        assert active_addr in body.get("message", ""), (
            f"{method} {path}: 409 must point at the ACTIVE {active_addr}: {body.get('message')}")
        assert body.get("group", {}).get("name") == GROUP_NAME, body
        assert body.get("group", {}).get("term") in (0, lease.get("term")), body
        members = body.get("group", {}).get("members") or []
        actives = [m for m in members if m.get("role") == "ACTIVE"]
        assert len(actives) == 1, f"{method} {path}: expected one ACTIVE member: {members}"
        if path == "/status":
            for field in ("state", "eventsRead", "eventsApplied"):
                assert field not in body, f"a STANDBY 409 must carry no pipeline field '{field}': {body}"
    for subcommand in ("status", "start", "pause", "resume", "finalize"):
        exit_code, stdout, stderr = standby.cli(subcommand)
        combined = f"{stdout}\n{stderr}"
        assert exit_code != 0, f"CLI '{subcommand}' on a STANDBY must fail: {combined}"
        assert "not_active" in combined, f"CLI '{subcommand}' missing not_active: {combined}"
    standby_metrics = standby.metrics()["data"]
    assert standby_metrics["percona_clustersync_mongodb_ha_active"] == 0
    active_metrics = active.metrics()["data"]
    assert active_metrics["percona_clustersync_mongodb_ha_active"] == 1
    assert active_metrics["percona_clustersync_mongodb_ha_term"] == lease.get("term")
    active_group = _status(active).get("group", {})
    assert active_group.get("term") == lease.get("term"), (
        f"the ACTIVE must report the current lease term: {active_group}")
    survivors = [inst for inst in group.instances if inst.name != active.name]
    with MetricsProbe(survivors) as probe:
        group.kill_active()
        group.wait_for_single_active()
        promoted = group.active()
        time.sleep(2)
    assert not probe.failures, f"/metrics was not continuously served: {probe.failures[:5]}"
    promoted_metrics = promoted.metrics()["data"]
    assert promoted_metrics["percona_clustersync_mongodb_ha_active"] == 1
    transitions = promoted_metrics["percona_clustersync_mongodb_ha_role_transitions_total"]
    assert transitions >= 1, f"promotion must count a role transition: {transitions}"
    # Back down to one live member: the group fields must disappear again.
    for inst in group.standbys():
        inst.kill()
    deadline = time.time() + 30
    while "group" in _status(promoted) and time.time() < deadline:
        time.sleep(1)
    body = _status(promoted)
    assert body.get("state"), f"the surviving ACTIVE stopped answering /status: {body}"
    for field in ("me", "role", "group"):
        assert field not in body, f"'{field}' must disappear once only one member is left: {body}"

@pytest.mark.parametrize(
    "state", ["idle", "cloning", "paused", "finalizing", "finalized", "failed"])
@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(900, func_only=True)
def test_ha_promotion_from_state_PML_T129(start_ha_cluster, src_cluster, dst_cluster, state):
    """
    A promoted instance restores the checkpointed state and stays usable.
    Promotion behaves differently per state, so each one is set up, waited
    for in the checkpoint and then the ACTIVE is killed. The bar is that the
    new ACTIVE reports the same state and can still be driven to a finalized,
    matching target. State 'cloning' is the exception: the takeover still has 
    to happen, but run can't continue, it should fail cleanly and re-clone on start.
    """
    group = start_ha_cluster
    operation_threads = []
    try:
        active = _drive_to_state(group, src_cluster, dst_cluster, state, operation_threads)
        if state not in ("idle", "cloning"):
            # 'idle' writes no checkpoint at all, and _drive_to_state already
            # waited for the 'cloning' one. wait_for_checkpoint() is no use
            # here: it waits for a finished clone, so read the state directly.
            checkpoints = _state_coll(dst_cluster, "checkpoints")
            deadline = time.time() + 60
            while not checkpoints.find_one({"_id": "pcsm", "data.state": state}):
                assert time.time() < deadline, (
                    f"state '{state}' was never checkpointed, last seen '{_status(active).get('state')}'")
                time.sleep(0.5)
        killed = group.kill_active()
        group.wait_for_single_active()
    finally:
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()
    promoted = group.active()
    assert promoted.name != killed.name, "a different instance must take over"
    # An interrupted clone cannot be carried across a promotion.
    expected = "failed" if state == "cloning" else state
    status = _wait_state(promoted, expected, timeout=90)
    assert status, f"promoted instance restored '{_status(promoted).get('state')}', expected '{expected}'"
    if state == "finalizing":
        # Finalization is not resumed on promotion by design, so it has to be re-issued.
        assert not promoted.start(), "/start must be refused while the state is 'finalizing'"
        # Bounded on purpose: this call currently hangs the instance.
        promoted.request("POST", "/finalize", {}, timeout=15)
        if not _wait_state(promoted, "finalized", timeout=90):
            pytest.xfail(
                "Known issue: PCSM-385. Re-issuing finalize after promotion deadlocks: "
                f"last state {_status(promoted).get('state')!r}")
        result, mismatch = compare_data(src_cluster, dst_cluster)
        assert result is True, f"data mismatch after re-finalizing: {mismatch}"
        return
    if state in ("cloning", "failed"):
        error = status.get("error") or ""
        assert error.strip(), f"promoted ACTIVE failed with an empty error: {status}"
        assert "clone" in error.lower(), f"unexpected failure reason: {error}"
        code, body = promoted.request("POST", "/resume", {"fromFailure": True})
        assert code != 200 or body.get("ok") is False, (
            f"an interrupted clone must not be resumable with --from-failure: {body}")
        assert promoted.start(), "/start after an interrupted clone was rejected"
        assert promoted.wait_for_repl_stage(timeout=600), "the re-clone did not complete"
    elif state == "paused":
        assert promoted.resume(), "resume rejected after promotion from 'paused'"
    elif state in ("idle", "finalized"):
        assert promoted.start(), f"/start rejected after promotion from '{state}'"
        assert promoted.wait_for_repl_stage(timeout=600), f"did not reach repl after '{state}'"
    assert promoted.wait_for_zero_lag(), "promoted ACTIVE failed to catch up"
    assert promoted.finalize(), "failed to finalize on the promoted ACTIVE"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch after promotion from '{state}': {mismatch}"

@pytest.mark.parametrize("fault", ["source_outage", "target_outage", "instance_partition"])
@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(900, func_only=True)
def test_ha_network_faults_PML_T130(start_ha_cluster, src_cluster, dst_cluster, fault):
    """
    HA under network faults rather than clean crashes.
    A target outage stops every instance renewing the lease at once. A source
    outage must not hand off, since no standby can do any better. A partitioned ACTIVE 
    is a would-be split brain: it has to step down, and once it is back it must not write.
    """
    group = start_ha_cluster
    # Only the partition case needs the writer.
    writer = CounterWriter(src_cluster) if fault == "instance_partition" else None
    operation_threads = []
    try:
        if writer:
            writer.seed()
        _, threads = create_all_types_db(
            src_cluster.connection, "init_test_db",
            start_crud=True, is_sharded=src_cluster.is_sharded)
        operation_threads += threads
        active = group.active()
        assert active.start(), "Failed to start csync on ACTIVE"
        assert active.wait_for_repl_stage(), "Failed to reach replication stage"
        if fault == "source_outage":
            # No standby can reach the source either, so a handover would only add recovery cost.
            term_before = _lease_doc(dst_cluster).get("term")
            src_cluster.network_interruption(delay=25)
            assert group.active().name == active.name, (
                "a source outage must not trigger a failover - no standby can do better")
            assert _lease_doc(dst_cluster).get("term") == term_before, (
                "the lease changed hands during a source outage")
        elif fault == "target_outage":
            # The lease lives on the target, so every instance loses it at once.
            dst_cluster.network_interruption(delay=25)
            group.wait_for_single_active(timeout=120)
            assert len(group.standbys()) == 2, "the group must converge on one ACTIVE again"
            assert _lease_doc(dst_cluster), "lease document did not come back after the outage"
        else:
            assert active.wait_for_checkpoint(), "clone completion was never checkpointed"
            writer.start()
            target_counter = pymongo.MongoClient(dst_cluster.connection)["ha_counter_db"]["counter"]
            # Build up a backlog before cutting the ACTIVE off.
            time.sleep(10)
            standby_names = {s.name for s in group.standbys()}
            active.disconnect_network()
            try:
                deadline = time.time() + 90
                while True:
                    promoted = next(
                        (i for i in group.instances
                         if i.name in standby_names and i.role() == "ACTIVE"),
                        None)
                    if promoted:
                        break
                    assert time.time() < deadline, (
                        "no standby took over while the ACTIVE was cut off")
                    time.sleep(1)
                # Keep writing across the takeover, so old ACTIVE still has work left when it comes back.
                time.sleep(30)
                writer.stop()
                stop_all_crud_operations()
                # The source is quiet now, so this is the last thing to replicate.
                writer.write_final_value()
                assert promoted.wait_for_zero_lag(timeout=300), "promoted ACTIVE failed to catch up"
                assert promoted.finalize(), "failed to finalize on the promoted ACTIVE"
                before = set(target_counter.distinct("value"))
                assert before == {FINAL_VALUE}, (
                    f"the new ACTIVE never wrote FINAL_VALUE, so an overwrite "
                    f"would not be detectable: target holds {sorted(before)}")
                result, mismatch = compare_data(src_cluster, dst_cluster)
                assert result is True, f"target diverged before the reconnect: {mismatch}"
                active.connect_network()
                assert active.wait_for_role("STANDBY", timeout=90), (
                    "a reconnected ACTIVE must step down")
                # Give it time to flush anything it was holding.
                time.sleep(20)
            finally:
                if not active.is_alive or active.role() is None:
                    active.connect_network()
            source_values = {doc["_id"]: doc["value"] for doc in writer.coll.find()}
            target_values = {doc["_id"]: doc["value"] for doc in target_counter.find()}
            stale = {
                doc_id: (value, target_values.get(doc_id))
                for doc_id, value in source_values.items()
                if target_values.get(doc_id) != value
            }
            for doc_id, (expected, got) in sorted(stale.items()):
                Cluster.log(f"counter _id={doc_id}: source has {expected}, target has {got}")
            result, mismatch = compare_data(src_cluster, dst_cluster)
            if stale or result is not True:
                # Known product issue: the checkpoint is the only thing guarded
                # by the lease term, so an instance that lost the lease can
                # still write. Nothing repairs it - the new ACTIVE will not
                # re-apply what it already applied.
                pytest.xfail(
                    f"'{active.name}' overwrote newer data after losing the lease: "
                    f"{len(stale)} of {len(source_values)} counter documents differ, "
                    f"source and target differ by {mismatch}")
            assert group.active().name == promoted.name, "reconnecting must not change the ACTIVE"
            assert len(group.standbys()) == 2
    finally:
        if writer:
            writer.close()
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()
    promoted = group.active()
    if _status(promoted).get("state") != "finalized":
        assert promoted.wait_for_zero_lag(timeout=600), f"failed to catch up after {fault}"
        assert promoted.finalize(), f"failed to finalize after {fault}"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch after {fault}: {mismatch}"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_ha_promoted_inherits_run_config_PML_T131(start_ha_cluster, src_cluster, dst_cluster):
    """
    A promoted instance inherits run configuration, not its own flags.
    Start options live in the checkpoint, not in the standby's command line.
    Check that promoted instance doesn't fall back to its own defaults.
    """
    group = start_ha_cluster
    operation_threads = []
    try:
        for db_name in ("included_db", "excluded_db"):
            _, threads = create_all_types_db(
                src_cluster.connection, db_name,
                start_crud=True, is_sharded=src_cluster.is_sharded)
            operation_threads += threads
        active = group.active()
        assert active.start(raw_args={"excludeNamespaces": ["excluded_db.*"]}), (
            "Failed to start filtered csync on ACTIVE")
        assert active.wait_for_repl_stage(), "Failed to reach replication stage"
        killed = group.kill_active()
        group.wait_for_single_active()
        promoted = group.active()
        assert _wait_state(promoted, "running", timeout=60), (
            f"promoted ACTIVE is {_status(promoted).get('state')}, not running"
        )
        assert promoted.name != killed.name, "a different instance must take over"
    finally:
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()
    promoted = group.active()
    assert promoted.wait_for_zero_lag(), "Promoted ACTIVE failed to catch up"
    assert promoted.finalize(), "Failed to finalize on the promoted ACTIVE"
    target = pymongo.MongoClient(dst_cluster.connection)
    assert "included_db" in target.list_database_names(), "Included namespace was not replicated"
    assert "excluded_db" not in target.list_database_names(), (
        "Promoted instance dropped the namespace filter and replicated excluded data")
