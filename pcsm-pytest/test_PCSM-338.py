import time

import pymongo
import pytest
from bson.timestamp import Timestamp

from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_PLM_T(start_cluster, src_cluster, dst_cluster, csync):
    """

    """
    operation_threads = []
    try:
        generate_dummy_data(src_cluster.connection, num_collections=15, is_sharded=src_cluster.is_sharded)
        _, operation_threads = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)

        # used to slow down clone process
        throttled_start_options = {
            "cloneNumParallelCollections": 1,
            "cloneNumReadWorkers": 1,
            "cloneNumInsertWorkers": 1,
            "cloneReadBatchSize": "16MiB",
        }

        assert csync.start(raw_args=throttled_start_options), "Failed to start csync service"
        assert csync.wait_for_checkpoint(), "Clustersync failed to save checkpoint"
        csync.container.stop()
        assert csync.restart(), "Failed to restart csync after stopping mid-catchup"

        dst_client = pymongo.MongoClient(dst_cluster.connection)
        doc = dst_client["percona_clustersync_mongodb"]["checkpoints"].find_one({"_id": "pcsm"})
        assert doc is not None, "No checkpoint document found"

        clone_subdoc = doc["data"]["clone"]
        assert "finishTS" in clone_subdoc, "Clone subdocument is missing the finishTS field entirely"
        finish_ts = clone_subdoc["finishTS"]
        assert isinstance(finish_ts, Timestamp) and finish_ts != Timestamp(0, 0), "Persisted clone checkpoint's finishTS is not valid"
        dst_client.close()
    finally:
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_pause_finalize_rejected_during_catchup(start_cluster, src_cluster, dst_cluster, csync):
    """

    """
    operation_threads = []
    try:
        generate_dummy_data(src_cluster.connection, num_collections=15, is_sharded=src_cluster.is_sharded)
        _, operation_threads = create_all_types_db(
            src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded
        )

        throttled_start_options = {
            "cloneNumInsertWorkers": 1,
            "replNumWorkers": 1,
            "replBulkOpsSize": 1,
            "pauseOnInitialSync": True,
        }
        assert csync.start(raw_args=throttled_start_options), "Failed to start csync service"
        assert csync.wait_for_checkpoint(), "Clustersync failed to save checkpoint"

        deadline = time.time() + 60
        data = None
        while time.time() < deadline:
            status = csync.status()
            data = status["data"]
            if data["initialSync"]["completed"] is False:
                break
            time.sleep(0.2)
        else:
            pytest.fail("Never observed initialSync.completed == False during throttled catchup")

        print(f"eventsRead={data.get('eventsRead')}, eventsApplied={data.get('eventsApplied')}")

        assert data["state"] != "paused", "pauseOnInitialSync should not have paused while initial sync is still incomplete"
        assert csync.finalize() is False, "/finalize should have been rejected while initial sync is incomplete"

        csync.container.stop()
        assert csync.restart(), "Failed to restart csync mid-catchup"

        status = csync.status()
        data = status["data"]
        assert data["initialSync"]["completed"] is False, "initialSync.completed was true immediately after restart while still mid-catchup"
        assert data["state"] != "paused", "pauseOnInitialSync auto-paused csync immediately after restart, even though initial sync is incomplete"

        assert csync.finalize() is False, "Expected /finalize to still be rejected after restart mid-catchup"
    finally:
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
