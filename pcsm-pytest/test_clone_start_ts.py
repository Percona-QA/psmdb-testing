import re
import time

import pymongo
import pytest
from bson.timestamp import Timestamp
from cluster import Cluster
from data_integrity_check import compare_data

# Wait this long with no writes so ping would return an old clusterTime.
# appendOplogNote writes a fresh pcsm:tick instead, so startTS is "now".
_IDLE_SECS = 3


def _last_oplog_ts(client):
    doc = client.local["oplog.rs"].find_one(sort=[("ts", -1)])
    assert doc is not None, "source local.oplog.rs is empty"
    return doc["ts"]


def _wait_for_idle_oplog(client, min_idle_secs=_IDLE_SECS, timeout=20):
    deadline = time.time() + timeout
    last_ts = _last_oplog_ts(client)
    while time.time() < deadline:
        last_ts = _last_oplog_ts(client)
        idle_secs = time.time() - last_ts.time
        if idle_secs >= min_idle_secs:
            return last_ts
        time.sleep(0.2)
    raise AssertionError(
        f"source oplog did not stay idle for {min_idle_secs}s within {timeout}s "
        f"(last ts={last_ts}, idle {time.time() - last_ts.time:.1f}s)")


def _parse_status_ts(value):
    if isinstance(value, Timestamp):
        return value
    if isinstance(value, dict) and "t" in value and "i" in value:
        return Timestamp(int(value["t"]), int(value["i"]))
    if isinstance(value, str) and "." in value:
        parts = value.split(".", 1)
        return Timestamp(int(parts[0]), int(parts[1]))
    return None


def _start_ts_from_status(csync):
    resp = csync.status()
    if not resp.get("success"):
        return None
    data = resp.get("data") or {}
    for keys in (("initialSync", "startTS"), ("clone", "startTS"), ("startTS",)):
        cur = data
        for key in keys:
            if not isinstance(cur, dict) or key not in cur:
                cur = None
                break
            cur = cur[key]
        parsed = _parse_status_ts(cur)
        if parsed is not None:
            return parsed
    return None


def _start_ts_from_checkpoint(dst):
    doc = dst["percona_clustersync_mongodb"]["checkpoints"].find_one({"_id": "pcsm"})
    if not doc:
        return None
    clone = (doc.get("data") or {}).get("clone") or {}
    start_ts = clone.get("startTS")
    if isinstance(start_ts, Timestamp) and start_ts.time:
        return start_ts
    return None


def _wait_for_clone_start_ts(csync, dst, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        start_ts = _start_ts_from_status(csync) or _start_ts_from_checkpoint(dst)
        if start_ts is not None:
            return start_ts
        time.sleep(0.2)
    raise AssertionError(
        "clone startTS was not exposed in /status or persisted as "
        "data.clone.startTS in the target checkpoint")


def _repl_checkpoint_op_ts(dst):
    doc = dst["percona_clustersync_mongodb"]["checkpoints"].find_one({"_id": "pcsm"})
    if not doc:
        return None
    ts = ((doc.get("data") or {}).get("repl") or {}).get("checkpointOpTS")
    if isinstance(ts, Timestamp) and ts.time:
        return ts
    return None


def _wait_for_repl_checkpoint_op_ts(dst, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        ts = _repl_checkpoint_op_ts(dst)
        if ts is not None:
            return ts
        time.sleep(0.2)
    raise AssertionError(
        "repl.checkpointOpTS was not persisted in the target checkpoint")


def _repl_started_from_log(csync, start_ts):
    """True if Repl.Start logged Change Replication started at start_ts."""
    logs = csync.logs(tail=None)
    pat = re.compile(
        rf"Change Replication started.*op_ts=\[{start_ts.time},\s*{start_ts.inc}\]"
        rf"|op_ts=\[{start_ts.time},\s*{start_ts.inc}\].*Change Replication started")
    return any(pat.search(line) for line in logs.splitlines())


@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
@pytest.mark.csync_env({"PCSM_RECOVERY_CHECKPOINT_INTERVAL": "1s"})
def test_csync_PML_T114(start_cluster, src_cluster, dst_cluster, csync):
    """
    PCSM-241: clone startTS is captured with appendOplogNote, not ping,
    and change-stream replication starts at that same timestamp.

    After /start the source oplog must contain op:n / o.msg=pcsm:tick at
    exactly the recorded startTS, and Repl.Start must open the stream at
    that same timestamp (log + checkpointOpTS).
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db = "csync_clone_start_ts"
    src[db]["docs"].insert_many([{"_id": i, "n": i} for i in range(50)])

    last_ts_before_start = _wait_for_idle_oplog(src)
    Cluster.log(
        f"Source idle: last oplog ts={last_ts_before_start} "
        f"({time.time() - last_ts_before_start.time:.1f}s ago)")

    assert csync.start(), "Failed to start csync service"

    start_ts = _wait_for_clone_start_ts(csync, dst)
    Cluster.log(f"Recorded clone startTS={start_ts} (status/checkpoint)")

    tick = src.local["oplog.rs"].find_one({
        "ts": start_ts, "op": "n", "o.msg": "pcsm:tick"})
    assert tick is not None, (
        f"source local.oplog.rs has no op:n pcsm:tick at recorded startTS "
        f"{start_ts} (last oplog before /start was {last_ts_before_start}); "
        "clone.go must capture startTS with appendOplogNote "
        "(mdb.AdvanceClusterTime), not ping (mdb.ClusterTime)")
    Cluster.log(f"Found oplog tick at {tick['ts']}: op={tick.get('op')} o={tick.get('o')}")

    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    repl_from = _wait_for_repl_checkpoint_op_ts(dst)
    Cluster.log(f"repl.checkpointOpTS={repl_from}")
    assert repl_from == start_ts, (
        f"replication resume frontier {repl_from} is not clone startTS "
        f"{start_ts}; Repl.Start must use SetStartAtOperationTime(startTS)")
    assert _repl_started_from_log(csync, start_ts), (
        f"'Change Replication started' was not logged at op_ts="
        f"[{start_ts.time},{start_ts.inc}]: {csync.logs(tail=80)}")

    src[db]["docs"].insert_one({"_id": "after-startTS", "n": 999})
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert dst[db]["docs"].find_one({"_id": "after-startTS"}) is not None, (
        "write after startTS was not replicated")
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
