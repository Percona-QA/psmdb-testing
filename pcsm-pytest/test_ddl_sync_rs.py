import pytest
import pymongo
import threading
import docker
import re

from cluster import Cluster
from clustersync import Clustersync
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data_rs


@pytest.fixture(scope="module")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="module")
def dstRS():
    return Cluster({ "_id": "rs2", "members": [{"host":"rs201"}]})

@pytest.fixture(scope="module")
def srcRS():
    return Cluster({ "_id": "rs1", "members": [{"host":"rs101"}]})

@pytest.fixture(scope="module")
def csync(srcRS,dstRS):
    return Clustersync('csync',srcRS.csync_connection, dstRS.csync_connection)

@pytest.fixture(scope="function")
def start_cluster(srcRS, dstRS, csync, request):
    log_marker = request.node.get_closest_marker("csync_log_level")
    log_level = log_marker.args[0] if log_marker and log_marker.args else "debug"
    env_marker = request.node.get_closest_marker("csync_env")
    env_vars = env_marker.args[0] if env_marker and env_marker.args else None
    try:
        srcRS.destroy()
        dstRS.destroy()
        csync.destroy()
        src_create_thread = threading.Thread(target=srcRS.create)
        dst_create_thread = threading.Thread(target=dstRS.create)
        src_create_thread.start()
        dst_create_thread.start()
        src_create_thread.join()
        dst_create_thread.join()
        csync.create(log_level=log_level, env_vars=env_vars)
        yield True
    finally:
        if request.config.getoption("--verbose"):
            logs = csync.logs()
            print(f"\n\ncsync Last 50 Logs for csync:\n{logs}\n\n")
        srcRS.destroy()
        dstRS.destroy()
        csync.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_rs_csync_PML_T9(start_cluster, srcRS, dstRS, csync):
    """
    Test to verify collection drop and re-creation during clone phase
    """
    src = pymongo.MongoClient(srcRS.connection)
    for i in range(5):
        src["dummy"].create_collection(f"collection_{i}", capped=True, size=2147483648, max=500000)
    generate_dummy_data(srcRS.connection, 'dummy', 5, 500000, drop_before_creation=False)
    for i in range(5):
        src["dummy"][f"collection_{i}"].create_index([("array", 1)])
    result = csync.start()
    assert result is True, "Failed to start csync service"
    for i in range(5):
        src["dummy"].command("collMod", f"collection_{i}", cappedSize=512 * 1024, cappedMax=500)
        src["dummy"][f"collection_{i}"].drop_indexes()
    for i in range(5):
        src["dummy"].drop_collection(f"collection_{i}")

    result = csync.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"
    result = csync.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_errors = ["NamespaceNotFound", "IndexNotFound", "QueryPlanKilled", "collection not found", "No indexes to create"]
    if not csync_error:
        unexpected = [line for line in error_logs if all(expected_error not in line for expected_error in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.timeout(300,func_only=True)
def test_rs_csync_PML_T10(start_cluster, srcRS, dstRS, csync):
    """
    Test to verify collection drop and re-creation replication phase
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = csync.start()
        assert result is True, "Failed to start csync service"
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        _, _ = create_all_types_db(srcRS.connection, "repl_test_db", create_ts=True)
        # Re-create data during replication phase by dropping and re-creating the collections
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "repl_test_db",
                                                                drop_before_creation=True, start_crud=True)
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

    result = csync.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "5"})
@pytest.mark.csync_log_level("trace")
def test_rs_csync_PML_T11(start_cluster, srcRS, dstRS, csync):
    """
    Test to verify DB drop and re-creation during clone phase
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        generate_dummy_data(srcRS.connection, "init_test_db")
        # Re-create data during clone phase by dropping DB
        def start_csync():
            result = csync.start()
            assert result is True, "Failed to start csync service"
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
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        for thread in all_threads:
            thread.join()

    result = csync.wait_for_repl_stage(timeout=30)
    if not result:
        if "Executor error during getMore" in csync.logs(tail=2000):
            pytest.xfail("Known issue: PCSM-147")
        else:
            assert False, "Failed to start replication stage"

    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_errors = ["QueryPlanKilled","RetryableWrite"]
    if not csync_error:
        unexpected = [line for line in error_logs if all(expected_error not in line for expected_error in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.timeout(300,func_only=True)
def test_rs_csync_PML_T12(start_cluster, srcRS, dstRS, csync):
    """
    Test to verify DB drop and re-creation during replication phase
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = csync.start()
        assert result is True, "Failed to start csync service"
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        _, _ = create_all_types_db(srcRS.connection, "repl_test_db")
        # Re-create data during replication phase by dropping DB
        src.drop_database("repl_test_db")
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)

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

    result = csync.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"