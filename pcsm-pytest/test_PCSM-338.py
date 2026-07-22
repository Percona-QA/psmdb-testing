import time

import pymongo
import pytest
from bson.timestamp import Timestamp

from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_PLM_T(start_cluster, src_cluster, dst_cluster, csync):
    """

    """
    operation_threads = []
    try:
        generate_dummy_data(src_cluster.connection, is_sharded=src_cluster.is_sharded)
        _, operation_threads = create_all_types_db(
            src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded
        )
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_checkpoint(), "Clustersync failed to save checkpoint"

        dst_client = pymongo.MongoClient(dst_cluster.connection)
        doc = dst_client["percona_clustersync_mongodb"]["checkpoints"].find_one({"_id": "pcsm"})
        assert doc is not None, "No checkpoint document found"
        finish_ts = doc["data"]["clone"].get("finishTS", Timestamp(0, 0))
        assert finish_ts != Timestamp(0, 0), (
            "Persisted clone checkpoint has a zero finishTS - PCSM-338 regression: "
            "recovering from this checkpoint would make PCSM falsely report initial "
            "sync as completed immediately after a restart, regardless of real "
            "catchup progress"
        )

        assert csync.restart(), "Failed to restart csync"
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()

    time.sleep(5)
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "detected concurrent process"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))
