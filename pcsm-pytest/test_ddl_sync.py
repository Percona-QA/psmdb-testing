import pytest
import pymongo
import threading
import re

from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "5"})
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T9(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to verify collection drop and re-creation during clone phase
    """
    src = pymongo.MongoClient(src_cluster.connection)
    is_sharded = src_cluster.layout == "sharded"
    generate_dummy_data(src_cluster.connection, 'dummy', 5, 200000, drop_before_creation=False, is_sharded=is_sharded)
    for i in range(5):
        src["dummy"].create_collection(f"test_collection_{i}", capped=True, size=2147483648, max=500000)
    for i in range(5):
        src["dummy"][f"test_collection_{i}"].create_index([("array", 1)])
    assert csync.start(), "Failed to start csync service"
    src["dummy"].drop_collection("collection_0")
    for i in range(5):
        src["dummy"].command("collMod", f"test_collection_{i}", cappedSize=512 * 1024, cappedMax=500)
        src["dummy"][f"test_collection_{i}"].drop_indexes()
    for i in range(5):
        src["dummy"].drop_collection(f"test_collection_{i}")
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_errors = ["NamespaceNotFound", "IndexNotFound", "QueryPlanKilled", "InvalidOptions", "collection not found", "No indexes to create"]
    if not csync_error:
        unexpected = [line for line in error_logs if all(expected_error not in line for expected_error in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T10(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to verify collection drop and re-creation replication phase
    """
    try:
        is_sharded = src_cluster.layout == "sharded"
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        _, _ = create_all_types_db(src_cluster.connection, "repl_test_db", create_ts=True, is_sharded=is_sharded)
        # Re-create data during replication phase by dropping and re-creating the collections
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "repl_test_db",
                                                                drop_before_creation=True, start_crud=True, is_sharded=is_sharded)
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        if "operation_threads_2" in locals():
            all_threads += operation_threads_2
        for thread in all_threads:
            thread.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "5"})
@pytest.mark.csync_log_level("trace")
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T11(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to verify DB drop and re-creation during clone phase
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"
        generate_dummy_data(src_cluster.connection, "init_test_db", is_sharded=is_sharded)
        # Re-create data during clone phase by dropping DB
        def start_csync():
            assert csync.start(), "Failed to start csync service"
        def delayed_drop():
            log_stream = csync.logs(stream=True)
            pattern = re.compile(r'read batch.*ns=init_test_db\.collection_0.*s=copy')
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if pattern.search(line):
                    break
            src.drop_database("init_test_db")
        t1 = threading.Thread(target=start_csync)
        t2 = threading.Thread(target=delayed_drop)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        for thread in all_threads:
            thread.join()
    assert csync.wait_for_repl_stage(timeout=30), "Failed to start replication stage"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_errors = ["QueryPlanKilled","RetryableWrite"]
    if not csync_error:
        unexpected = [line for line in error_logs if all(expected_error not in line for expected_error in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T12(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to verify DB drop and re-creation during replication phase
    """
    try:
        is_sharded = src_cluster.layout == "sharded"
        src = pymongo.MongoClient(src_cluster.connection)
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        _, _ = create_all_types_db(src_cluster.connection, "repl_test_db", is_sharded=is_sharded)
        # Re-create data during replication phase by dropping DB
        src.drop_database("repl_test_db")
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        if "operation_threads_2" in locals():
            all_threads += operation_threads_2
        for thread in all_threads:
            thread.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"