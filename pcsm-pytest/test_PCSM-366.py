import time

import pymongo
import pytest
from clustersync import Clustersync
from data_generator import generate_dummy_data
from data_integrity_check import compare_data

BATCH_DOCS = 40
APPLY_DELAY_MS = 25000
SETTLE_SECONDS = 4

@pytest.fixture(scope="function")
def csync(src_cluster, dst_cluster):
    return Clustersync("csync", src_cluster.csync_connection + "&appName=pcsm",
                       dst_cluster.csync_connection + "&appName=pcsm")

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.timeout(600, func_only=True)
def test_rs_pause_reports_pausing_until_replication_stops_PML_T145(start_cluster, src_cluster, dst_cluster, csync):
    """PCSM-366: pause reports 'pausing' while queued writes drain and 'paused' only after
    replication has stopped; finalize during the drain fails without scheduling finalization."""
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    target_coll = dst["db_pausing"]["coll"]
    try:
        generate_dummy_data(src_cluster.connection, num_collections=3, is_sharded=src_cluster.is_sharded)
        assert csync.start(raw_args={"replNumWorkers": 1, "replBulkOpsSize": 10}), "Failed to start csync"
        assert csync.wait_for_repl_stage(), "Failed to reach the replication stage"
        # Hold replication data writes only, target 8.0+ applies them with bulkWrite, while
        # older targets replay inserts as upserting updates, so set hold to test collection
        failpoint = {"failCommands": ["bulkWrite"], "blockConnection": True,
                     "blockTimeMS": APPLY_DELAY_MS, "appName": "pcsm"}
        if dst.server_info()["versionArray"][0] < 8:
            failpoint.update({"failCommands": ["update"], "namespace": "db_pausing.coll"})
        read_before = csync.status()["data"].get("eventsRead", 0)
        dst.admin.command({"configureFailPoint": "failCommand", "mode": "alwaysOn", "data": failpoint})
        src["db_pausing"]["coll"].insert_many([{"_id": i} for i in range(BATCH_DOCS)])
        for _ in range(60):
            if csync.status()["data"].get("eventsRead", 0) >= read_before + BATCH_DOCS:
                break
            time.sleep(1)
        else:
            pytest.fail("PCSM did not read the pending change events")
        blocked_at = time.time()
        time.sleep(SETTLE_SECONDS)
        assert target_coll.count_documents({}) == 0, "Failpoint did not hold replication writes"
        assert csync.role() == "ACTIVE", "Instance lost its lease while target writes were held"
        # When pause is accepted while a replication write cannot finish
        code, body = csync.request("POST", "/pause", {})
        assert code == 200 and isinstance(body, dict) and body.get("ok") is True, f"Pause was not accepted: {body}"
        # Status reports pausing, not paused
        data = csync.status()["data"]
        assert data.get("state") == "pausing", f"Expected 'pausing' right after pause, got '{data.get('state')}'"
        assert data.get("info") == "Pausing Change Replication", f"Unexpected info while pausing: {data.get('info')}"
        # Repeated pause is idempotent, start is rejected
        code, body = csync.request("POST", "/pause", {})
        assert code == 200 and isinstance(body, dict) and body.get("ok") is True, f"Repeated pause must be accepted while pausing: {body}"
        code, body = csync.request("POST", "/start", {})
        assert code == 200 and isinstance(body, dict) and body.get("ok") is False \
            and "already running" in body.get("error", ""), (
            f"Start must be rejected as already running while pausing: {body}")
        # Finalize outlives its request deadline during drain: it must fail,
        # leave the pause running and schedule no finalization
        code, body = csync.request("POST", "/finalize", {}, timeout=30)
        assert code == 200 and isinstance(body, dict) and body.get("ok") is False, f"Finalize must fail while the pause is still draining: {body}"
        data = csync.status()["data"]
        assert data.get("state") == "pausing", f"Expected 'pausing' after the finalize timeout, got '{data.get('state')}'"
        assert "finalization" not in data, f"Finalization must not be scheduled: {data.get('finalization')}"
        assert csync.role() == "ACTIVE", "Instance lost its lease while finalize waited for the pause"
        # Release held writes and only then expect paused
        dst.admin.command({"configureFailPoint": "failCommand", "mode": "off"})
        for _ in range(120):
            data = csync.status()["data"]
            assert data.get("state") != "failed", f"Csync failed while draining the pause: {data.get('error')}"
            if data.get("state") == "paused":
                break
            time.sleep(1)
        else:
            pytest.fail(f"Timed out waiting for 'paused', last state: {data.get('state')}")
        # Nothing may still be applied once paused is reported
        count_at_paused = target_coll.count_documents({})
        time.sleep(max(SETTLE_SECONDS, blocked_at + APPLY_DELAY_MS / 1000 + 2 - time.time()))
        assert target_coll.count_documents({}) == count_at_paused, "Target kept changing after 'paused'"
        assert csync.status()["data"].get("state") == "paused", "State must stay paused"
        # Replication carries on after resume
        assert csync.resume(), "Failed to resume csync"
        src["db_pausing"]["coll"].insert_many([{"_id": BATCH_DOCS + i} for i in range(BATCH_DOCS)])
        assert csync.wait_for_zero_lag(), "Failed to catch up on replication after resume"
        assert csync.pause(), "Failed to pause csync after resume"
        assert csync.status()["data"].get("state") == "paused", "Unheld pause must end in 'paused'"
    finally:
        dst.admin.command({"configureFailPoint": "failCommand", "mode": "off"})

    assert csync.finalize(), "Failed to finalize csync service after the pause completed"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Data mismatch after pause, resume and finalize: {mismatch}"
    src.close()
    dst.close()
