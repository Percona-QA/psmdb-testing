import threading
import time

import pymongo
import pytest
from cluster import Cluster
from clustersync import Clustersync
from data_generator import generate_dummy_data
from data_integrity_check import compare_data

LAG_DB = "db_backlog"
LAG_COLL = "load"
SUSTAIN_COLL = "sustain"
SEED_DB = "db_seed"
BACKLOG_DOCS = 20000
APPLY_DELAY_MS = 5000
QUIET_SECONDS = 3
TAIL_SECONDS = 15
FROZEN_SECONDS = 3
SAMPLE_INTERVAL = 1

THROTTLED_START_OPTIONS = {
    "cloneNumParallelCollections": 1,
    "cloneNumReadWorkers": 1,
    "cloneNumInsertWorkers": 1,
    "replEventQueueSize": 2000000,
    "replWorkerQueueSize": 2000000,
    "replNumWorkers": 1,
    "replBulkOpsSize": 10,
}

@pytest.fixture(scope="function")
def csync(src_cluster, dst_cluster):
    return Clustersync("csync", src_cluster.csync_connection + "&appName=pcsm",
                       dst_cluster.csync_connection + "&appName=pcsm")

def _write_backlog(connection):
    client = pymongo.MongoClient(connection)
    collection = client[LAG_DB][LAG_COLL]
    for seq in range(0, BACKLOG_DOCS, 500):
        docs = [{"_id": seq + i, "payload": "x" * 200} for i in range(500)]
        collection.insert_many(docs, ordered=False)
    client.close()

def _sustain_source_writes(connection, stop_event):
    client = pymongo.MongoClient(connection)
    collection = client[LAG_DB][SUSTAIN_COLL]
    seq = 0
    while not stop_event.is_set():
        docs = [{"_id": seq + i, "payload": "y" * 200} for i in range(100)]
        try:
            collection.insert_many(docs, ordered=False)
        except pymongo.errors.PyMongoError as e:
            Cluster.log(f"Sustain writer: {e}")
            time.sleep(0.1)
            continue
        seq += 100
        time.sleep(0.1)
    client.close()

def _slow_target_apply(connection):
    client = pymongo.MongoClient(connection)
    result = client.admin.command({
        "configureFailPoint": "failCommand",
        "mode": "alwaysOn",
        "data": {
            "failCommands": ["insert", "update", "delete", "bulkWrite"],
            "blockConnection": True,
            "blockTimeMS": APPLY_DELAY_MS,
            "appName": "pcsm",
        },
    })
    Cluster.log(f"Failpoint delaying PCSM target writes by {APPLY_DELAY_MS}ms: {result}")
    client.close()

def _disable_failpoint(connection):
    client = pymongo.MongoClient(connection)
    client.admin.command({"configureFailPoint": "failCommand", "mode": "off"})
    client.close()

def _status_data(csync, deadline):
    last_error = None
    while time.time() < deadline:
        response = csync.status()
        if response.get("success"):
            data = response.get("data")
            if data is None:
                last_error = "status response had no data"
            elif data.get("ok") is False:
                raise AssertionError(f"Csync reported an error: {data.get('error')}")
            else:
                return data
        else:
            last_error = response.get("error", "Failed to execute csync status command")
        time.sleep(0.1)
    raise AssertionError(f"Failed to read csync status: {last_error}")

def _wait_until(csync, predicate, timeout, message):
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = _status_data(csync, deadline)
        if predicate(data):
            return data
        time.sleep(0.1)
    raise AssertionError(message)

def _sample_catchup(csync, src_connection, dst_connection):
    dst_client = pymongo.MongoClient(dst_connection)
    collection = dst_client[LAG_DB][LAG_COLL]
    samples = []
    started = time.time()
    deadline = started + 180
    completed_at = None
    stop_sustain = threading.Event()
    sustain = None
    try:
        while time.time() < deadline:
            elapsed = time.time() - started
            if sustain is None and elapsed >= QUIET_SECONDS:
                Cluster.log("Resuming source writes and target apply")
                _disable_failpoint(dst_connection)
                sustain = threading.Thread(target=_sustain_source_writes,
                                           args=(src_connection, stop_sustain))
                sustain.start()
            data = _status_data(csync, deadline)
            initial_sync = data.get("initialSync") or {}
            sample = {
                "at": round(elapsed, 1),
                "lag": data.get("lagTimeSeconds"),
                "optime": str((data.get("lastReplicatedOpTime") or {}).get("ts")),
                "applied": data.get("eventsApplied") or 0,
                "completed": bool(initial_sync.get("completed")),
                "target_docs": collection.estimated_document_count(),
            }
            samples.append(sample)
            if sample["completed"] and completed_at is None:
                completed_at = time.time()
            if completed_at is not None and time.time() - completed_at >= TAIL_SECONDS:
                break
            time.sleep(SAMPLE_INTERVAL)
    finally:
        stop_sustain.set()
        if sustain is not None:
            sustain.join()
        dst_client.close()
    return samples


def _report(samples, backlog_docs):
    Cluster.log(f"Catch-up of {backlog_docs} backlog documents:")
    Cluster.log(f"{'t+s':>7} {'lagTimeSeconds':>15} {'lastReplicatedOpTime':>22} "
                f"{'eventsApplied':>14} {'targetDocs':>11} {'completed':>10}")
    for s in samples:
        Cluster.log(f"{s['at']:>7} {s['lag']!s:>15} {s['optime']:>22} "
                    f"{s['applied']:>14} {s['target_docs']:>11} {s['completed']!s:>10}")


def _longest_frozen_window(samples):
    """Longest stretch where lastReplicatedOpTime did not move. Flat applied
    samples stay in the window; it ends only when the timestamp advances."""
    best, current = [], []
    for sample in samples:
        if current and sample["optime"] == current[-1]["optime"]:
            current.append(sample)
            continue
        if _frozen_violation(current) and len(current) > len(best):
            best = current
        current = [sample]
    if _frozen_violation(current) and len(current) > len(best):
        best = current
    return best


def _frozen_violation(window):
    if len(window) < 2:
        return False
    grew = window[-1]["applied"] > window[0]["applied"]
    duration = window[-1]["at"] - window[0]["at"]
    return grew and duration >= FROZEN_SECONDS

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.timeout(800, func_only=True)
def test_rs_csync_lag_frozen_during_catchup_PML_T118(start_cluster, src_cluster, dst_cluster, csync):
    """PCSM-383: after clone, lastReplicatedOpTime must move as oplog catch-up applies."""
    generate_dummy_data(src_cluster.connection, SEED_DB, 2, 800000, 10000)
    assert csync.start(raw_args=THROTTLED_START_OPTIONS) is True, "Failed to start csync"

    _wait_until(csync, lambda d: (d.get("initialSync") or {}).get("clonedSizeBytes", 0) > 0,
                180, "Timed out waiting for the clone to start")
    _write_backlog(src_cluster.connection)
    data = _status_data(csync, time.time() + 30)
    assert not (data.get("initialSync") or {}).get("cloneCompleted"), (
        "Clone finished while the backlog was still being written")

    src_client = pymongo.MongoClient(src_cluster.connection)
    backlog_docs = src_client[LAG_DB][LAG_COLL].count_documents({})
    src_client.close()
    Cluster.log(f"Backlog built during the clone: {backlog_docs} documents in {LAG_DB}.{LAG_COLL}")

    _wait_until(csync, lambda d: (d.get("initialSync") or {}).get("cloneCompleted"),
                600, "Timed out waiting for the clone to complete")
    _slow_target_apply(dst_cluster.connection)
    try:
        samples = _sample_catchup(csync, src_cluster.connection, dst_cluster.connection)
    finally:
        _disable_failpoint(dst_cluster.connection)

    _report(samples, backlog_docs)

    assert csync.wait_for_zero_lag(timeout=300), "Failed to catch up on replication"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Lost or mismatched records after catch-up: {mismatch}"

    problems = []
    completed = next((s for s in samples if s["completed"]), None)
    if completed is None:
        problems.append("initialSync.completed never became true within the sampling window")
    elif completed["target_docs"] < backlog_docs:
        problems.append(
            f"initialSync.completed went true at t+{completed['at']}s reporting "
            f"lagTimeSeconds={completed['lag']}, but {backlog_docs - completed['target_docs']} of "
            f"{backlog_docs} backlog documents were still missing from the target")

    unapplied = [s for s in samples if s["target_docs"] < backlog_docs and s["lag"] is not None]
    if unapplied:
        lowest = min(unapplied, key=lambda s: s["lag"])
        if lowest["lag"] <= 5:
            problems.append(
                f"Reported lagTimeSeconds fell to {lowest['lag']} at t+{lowest['at']}s while "
                f"{backlog_docs - lowest['target_docs']} of {backlog_docs} backlog documents "
                f"were still unapplied")

    frozen = _longest_frozen_window(samples)
    if _frozen_violation(frozen):
        span = round(frozen[-1]["at"] - frozen[0]["at"], 1)
        applied = frozen[-1]["applied"] - frozen[0]["applied"]
        first_lag, last_lag = frozen[0]["lag"], frozen[-1]["lag"]
        rate = (last_lag - first_lag) / max(span, 1) if None not in (first_lag, last_lag) else 0
        problems.append(
            f"lastReplicatedOpTime stood still at {frozen[0]['optime']} from t+{frozen[0]['at']}s to "
            f"t+{frozen[-1]['at']}s ({span}s) while {applied} events were applied, "
            f"lagTimeSeconds {first_lag} -> {last_lag} ({rate:+.2f}s per second)")

    assert not problems, "PCSM-383 reproduced:\n" + "\n".join(f"  - {p}" for p in problems)
