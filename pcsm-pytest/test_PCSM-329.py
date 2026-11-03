import time

import pymongo
import pytest
from cluster import Cluster
from clustersync import Clustersync
from data_generator import generate_dummy_data
from data_integrity_check import compare_data

NS_DB = "dummy"
NS_COLL = "collection_0"
NAMESPACE = f"{NS_DB}.{NS_COLL}"
MAX_TIME_MS_EXPIRED = 50
DOC_COUNT = 25000
BATCH_SIZE = 10000
COPY_ERROR_LOG = "Failed to copy documents for collection"
TRANSIENT_ERROR_LOG = "Transient error"
EXPIRED_ERROR = "MaxTimeMSExpired"
OUTCOME_TIMEOUT = 30
SAMPLE_INTERVAL = 5

@pytest.fixture(scope="function")
def csync(src_cluster, dst_cluster):
    return Clustersync("csync", src_cluster.csync_connection + "&appName=pcsm",
                       dst_cluster.csync_connection + "&appName=pcsm")


def _fail_getmore(connection, mode):
    """Reject clone's data cursor getMore with MaxTimeMSExpired. """
    client = pymongo.MongoClient(connection)
    result = client.admin.command({
        "configureFailPoint": "failCommand",
        "mode": mode,
        "data": {
            "failCommands": ["getMore"],
            "errorCode": MAX_TIME_MS_EXPIRED,
            "namespace": NAMESPACE,
            "appName": "pcsm"}})
    Cluster.log(f"Failpoint rejecting getMore on {NAMESPACE} ({mode}): {result}")
    client.close()


def _disable_failpoint(connection):
    client = pymongo.MongoClient(connection)
    client.admin.command({"configureFailPoint": "failCommand", "mode": "off"})
    client.close()


def _sample(csync, started):
    response = csync.status()
    if not response.get("success"):
        return {"at": round(time.time() - started, 1), "state": "unreachable",
                "clone_completed": False, "cloned_bytes": 0,
                "error": response.get("error")}
    data = response.get("data") or {}
    initial_sync = data.get("initialSync") or {}
    return {
        "at": round(time.time() - started, 1),
        "state": data.get("state"),
        "clone_completed": bool(initial_sync.get("cloneCompleted")),
        "cloned_bytes": initial_sync.get("clonedSizeBytes", 0),
        "error": data.get("error")}


def _copy_failure_reached(logs):
    """The clone logged a copy failure for this namespace. """
    return any(COPY_ERROR_LOG in line and NAMESPACE in line for line in logs.splitlines())


def _report(samples):
    Cluster.log("PCSM status while the clone's getMore was being rejected:")
    Cluster.log(f"{'t+s':>7} {'state':>12} {'cloneCompleted':>15} "
                f"{'clonedSizeBytes':>16}  error")
    for s in samples:
        Cluster.log(f"{s['at']:>7} {s['state']!s:>12} {s['clone_completed']!s:>15} "
                    f"{s['cloned_bytes']!s:>16}  {s['error'] or ''}")


@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.timeout(600, func_only=True)
@pytest.mark.parametrize("scenario", [
    pytest.param({"mode": {"times": 1}, "may_recover": True}, id="first_getMore"),
    pytest.param({"mode": "alwaysOn", "may_recover": False}, id="every_getMore")])
def test_rs_csync_clone_getmore_error_reaches_failed_PML_T141(
        start_cluster, src_cluster, dst_cluster, csync, scenario):
    """PCSM-329: a clone read that cannot be retried must fail the pipeline. """
    generate_dummy_data(src_cluster.connection, NS_DB, num_collections=1,
                        doc_size=DOC_COUNT, batch_size=BATCH_SIZE)
    _fail_getmore(src_cluster.connection, scenario["mode"])
    samples = []
    outcome = None
    try:
        assert csync.start(), "Failed to start csync service"
        started = time.time()
        deadline = started + OUTCOME_TIMEOUT
        while time.time() < deadline:
            sample = _sample(csync, started)
            samples.append(sample)
            if sample["state"] == "failed":
                outcome = "failed"
                break
            if sample["clone_completed"]:
                outcome = "cloned"
                break
            time.sleep(SAMPLE_INTERVAL)
    finally:
        _disable_failpoint(src_cluster.connection)
    _report(samples)
    logs = csync.logs(tail=None)
    last = samples[-1] if samples else {}
    copy_failed = _copy_failure_reached(logs)
    retried = TRANSIENT_ERROR_LOG in logs
    expired = EXPIRED_ERROR in logs or EXPIRED_ERROR in (last.get("error") or "")
    if outcome == "failed":
        error = last.get("error") or ""
        assert error, "PCSM reported state='failed' without an error message"
        assert copy_failed and EXPIRED_ERROR in error, (
            f"PCSM reported state='failed' ({error}) without the injected getMore "
            f"on {NAMESPACE}: expected {COPY_ERROR_LOG!r} in the logs and "
            f"{EXPIRED_ERROR} in the status error")
        Cluster.log(f"PCSM failed the run as expected: {error}")
        return
    if outcome == "cloned":
        assert scenario["may_recover"], (
            "The clone completed while the source was still rejecting every getMore")
        assert expired and (copy_failed or retried), (
            f"The clone completed without {EXPIRED_ERROR} on {NAMESPACE}, "
            "so the getMore failpoint never reached PCSM")
        result, summary = compare_data(src_cluster, dst_cluster)
        assert result is True, f"Data mismatch after the retried getMore: {summary}"
        Cluster.log("PCSM retried rejected getMore and target matches source data")
        return
    assert expired and (copy_failed or retried), (
        f"PCSM made no progress and never logged {EXPIRED_ERROR} for {NAMESPACE}, "
        f"so the getMore failpoint never reached it. Last status: {last}")
    retry_detail = (
        f"{TRANSIENT_ERROR_LOG!r} was never logged, so retries did not start"
        if copy_failed and not retried
        else f"{TRANSIENT_ERROR_LOG!r} was logged")
    pytest.fail(
        f"PCSM-329 reproduced: the clone read of {NAMESPACE} hit {EXPIRED_ERROR} "
        f"({retry_detail}), but after {OUTCOME_TIMEOUT}s PCSM is still in "
        f"state={last.get('state')!r} with clonedSizeBytes={last.get('cloned_bytes')} "
        f"and cloneCompleted=False. The run never reached state='failed'")
