import pytest
import pymongo
import time

from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T28(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM behavior when restarted during the clone and replication stages
    """
    try:
        generate_dummy_data(src_cluster.connection, is_sharded=src_cluster.is_sharded)
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(), "Failed to start csync service"
        clone_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        # restart during clone stage
        csync.restart(reset=True)
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        result = csync.wait_for_checkpoint()
        assert result is True, "Clustersync failed to save checkpoint"
        repl_test_db, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        if "operation_threads_2" in locals():
            all_threads += operation_threads_2
        if "operation_threads_3" in locals():
            all_threads += operation_threads_3
        for thread in all_threads:
            thread.join()
    # restart during replication stage
    csync.restart()
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

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T29(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM pause/resume options
    """
    try:
        generate_dummy_data(src_cluster.connection, is_sharded=src_cluster.is_sharded)
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(), "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        result = csync.pause()
        assert result is False, "Can't pause csync service during clone stage"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        result = csync.pause()
        csync.restart()
        assert result is True, "Replication is paused"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        result = csync.resume()
        assert result is True, "Replication is resumed"
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        if "operation_threads_2" in locals():
            all_threads += operation_threads_2
        if "operation_threads_3" in locals():
            all_threads += operation_threads_3
        for thread in all_threads:
            thread.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_errors = ["Change Replication is not runnning", "detected concurrent process"]
    if not csync_error:
        unexpected = [line for line in error_logs if not any(expected in line for expected in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T37(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM when it's not possible to resume due to lost oplog history
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        if src_cluster.is_sharded:
            for shard in src_cluster.config['shards']:
                shard_primary = shard['members'][0]['host']
                shard_rs = shard['_id']
                shard_client = pymongo.MongoClient(f"mongodb://root:root@{shard_primary}:27017/?replicaSet={shard_rs}")
                result = shard_client.admin.command("replSetResizeOplog", size=990)
                assert result.get("ok") == 1.0, f"Failed to resize oplog on {shard_primary}: {result}"
        else:
            result = src.admin.command("replSetResizeOplog", size=990)
            assert result.get("ok") == 1.0, f"Failed to resize oplog: {result}"
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        result = csync.pause()
        assert result is True, "Replication is paused"
        generate_dummy_data(src_cluster.connection, "dummy", 15, 300000, is_sharded=src_cluster.is_sharded)
        result = csync.resume()
        assert result is True, "Replication is resumed"
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        for thread in all_threads:
            thread.join()
    result = csync.wait_for_zero_lag()
    if not result:
        assert "oplog history is lost" in csync.logs()
    status = csync.status()
    assert not status['data']['ok']
    assert status['data']['state'] != 'running'

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T38(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check how PCSM handles errors during recovery - if PCSM "incorrectly" replays previously applied
    operations after the restart, the first insert will fail due to an existing index that prohibits such doc
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    if src_cluster.is_sharded:
        src.admin.command("enableSharding", "test_db1")
        src.admin.command("shardCollection", "test_db1.test_collection", key={"_id": "hashed"})
    else:
        src["test_db1"].create_collection("test_collection")
    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    assert csync.wait_for_checkpoint(), "Clustersync failed to save checkpoint"
    result = src["test_db1"].test_collection.insert_one({"a": {"b": []}, "words": "omnibus"})
    # For sharded collections, delete_one must use _id or shard key
    if src_cluster.is_sharded:
        src["test_db1"].test_collection.delete_one({"_id": result.inserted_id})
    else:
        src["test_db1"].test_collection.delete_one({"a.b": [], "words": "omnibus"})
    src["test_db1"].test_collection.create_index([("a.b", 1), ("words", "text")],name="my_custom_index1")
    while time.time() < time.time() + 20:
        try:
            indexes = dst["test_db1"].test_collection.index_information()
            if "my_custom_index1" in indexes:
                break
        except Exception:
            pass
        time.sleep(0.5)
    result = csync.restart()
    assert result is True, "Failed to restart csync service"
    result = csync.wait_for_zero_lag()
    if not result:
        assert "text index contains an array in document" in csync.logs()
        pytest.xfail("Known issue: PCSM-128")
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "detected concurrent process"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))