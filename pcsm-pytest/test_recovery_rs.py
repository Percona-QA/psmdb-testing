import pytest
import pymongo
import docker
import threading

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
        csync.create()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            logs = csync.logs()
            print(f"\n\ncsync Last 50 Logs for csync:\n{logs}\n\n")
        srcRS.destroy()
        dstRS.destroy()
        csync.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_rs_csync_PML_T28(start_cluster, srcRS, dstRS, csync):
    """
    Test to check PCSM behavior when restarted during the clone and replication stages
    """
    try:
        generate_dummy_data(srcRS.connection)
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = csync.start()
        assert result is True, "Failed to start csync service"
        clone_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        # restart during clone stage
        csync.restart(reset=True)
        result = csync.start()
        assert result is True, "Failed to start csync service"
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = csync.wait_for_checkpoint()
        assert result is True, "Clustersync failed to save checkpoint"
        repl_test_db, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        # restart during replication stage
        csync.restart()
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
    result = csync.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "detected concurrent process"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.timeout(300,func_only=True)
def test_rs_csync_PML_T29(start_cluster, srcRS, dstRS, csync):
    """
    Test to check PCSM pause/resume options
    """
    try:
        generate_dummy_data(srcRS.connection)
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = csync.start()
        assert result is True, "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = csync.pause()
        assert result is False, "Can't pause csync service during clone stage"
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = csync.pause()
        csync.restart()
        assert result is True, "Replication is paused"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
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
    result = csync.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_errors = ["Change Replication is not runnning", "detected concurrent process"]
    if not csync_error:
        unexpected = [line for line in error_logs if not any(expected in line for expected in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.timeout(300,func_only=True)
def test_rs_csync_PML_T37(start_cluster, srcRS, dstRS, csync):
    """
    Test to check PCSM when it's not possible to resume due to lost oplog history
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        result = src.admin.command("replSetResizeOplog", size=990)
        assert result.get("ok") == 1.0, f"Failed to resize oplog: {result}"
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = csync.start()
        assert result is True, "Failed to start csync service"
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = csync.pause()
        assert result is True, "Replication is paused"
        generate_dummy_data(srcRS.connection, "dummy", 15, 300000)
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

@pytest.mark.timeout(300,func_only=True)
def test_rs_csync_PML_T38(start_cluster, srcRS, dstRS, csync):
    """
    Test to check how PCSM handles errors during recovery - if PCSM "incorrectly" replays previously applied
    operations after the restart, the first insert will fail due to an existing index that prohibits such doc
    """
    src = pymongo.MongoClient(srcRS.connection)
    src["test_db1"].create_collection("test_collection")
    src["test_db2"].create_collection("test_collection")
    result = csync.start()
    assert result is True, "Failed to start csync service"
    result = csync.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"
    result = csync.wait_for_checkpoint()
    assert result is True, "Clustersync failed to save checkpoint"
    src["test_db1"].test_collection.insert_one({"a": {"b": []}, "words": "omnibus"})
    src["test_db1"].test_collection.delete_one({"a.b": [], "words": "omnibus"})
    src["test_db1"].test_collection.create_index([("a.b", 1), ("words", "text")],name="my_custom_index1")
    result = csync.restart()
    assert result is True, "Failed to restart csync service"
    result = csync.wait_for_zero_lag()
    if not result:
        assert "text index contains an array in document" in csync.logs()
        pytest.xfail("Known issue: PCSM-128")
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "detected concurrent process"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))