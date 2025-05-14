import pytest
import pymongo
import time
import docker

from cluster import Cluster
from mongolink import Mongolink
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
def mlink(srcRS,dstRS):
    return Mongolink('mlink',srcRS.mlink_connection, dstRS.mlink_connection)

@pytest.fixture(scope="module")
def start_cluster(srcRS, dstRS, mlink, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        srcRS.create()
        dstRS.create()
        yield True

    finally:
        srcRS.destroy()
        dstRS.destroy()
        mlink.destroy()

@pytest.fixture(scope="function")
def reset_state(srcRS, dstRS, mlink, request):
    src_client = pymongo.MongoClient(srcRS.connection)
    dst_client = pymongo.MongoClient(dstRS.connection)
    def print_logs():
        if request.config.getoption("--verbose"):
            logs = mlink.logs()
            print(f"\n\nmlink Last 50 Logs for mlink:\n{logs}\n\n")
    request.addfinalizer(print_logs)
    mlink.destroy()
    for db_name in src_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            src_client.drop_database(db_name)
    for db_name in dst_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            dst_client.drop_database(db_name)
    mlink.create()

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T28(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML behavior when restarted during the clone and replication stages
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)
        generate_dummy_data(srcRS.connection)
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        clone_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        # restart during clone stage
        mlink.restart()
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = mlink.wait_for_checkpoint()
        assert result is True, "Mongolink failed to save checkpoint"
        repl_test_db, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        # restart during replication stage
        mlink.restart()
        time.sleep(5)
    except Exception as e:
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
    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    expected_error = "detected concurrent process"
    if not mlink_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T29(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML pause/resume options
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)
        generate_dummy_data(srcRS.connection)
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        clone_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = mlink.pause()
        assert result is False, "Can't pause mlink service during clone stage"
        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = mlink.pause()
        assert result is True, "Replication is paused"
        repl_test_db, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        time.sleep(5)
        result = mlink.resume()
        assert result is True, "Replication is resumed"
    except Exception as e:
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
    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    expected_error = "cannot pause: Change Replication is not runnning"
    if not mlink_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T37(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML when it's not possible to resume due to lost oplog history
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)
        result = src.admin.command("replSetResizeOplog", size=990)
        assert result.get("ok") == 1.0, f"Failed to resize oplog: {result}"
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = mlink.pause()
        assert result is True, "Replication is paused"
        generate_dummy_data(srcRS.connection, "dummy", 15, 300000)
        result = mlink.resume()
        assert result is True, "Replication is resumed"
    except Exception as e:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        for thread in all_threads:
            thread.join()
    result = mlink.wait_for_zero_lag()
    if not result:
        assert "oplog history is lost" in mlink.logs()
    status = mlink.status()
    assert status['data']['ok'] == False
    assert status['data']['state'] != 'running'

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T38(reset_state, srcRS, dstRS, mlink):
    """
    Test to check how PML handles errors during recovery - if PML "incorrectly" replays previously applied
    operations after the restart, the first insert will fail due to an existing index that prohibits such doc
    """
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)
    src["test_db1"].create_collection("test_collection")
    src["test_db2"].create_collection("test_collection")
    result = mlink.start()
    assert result is True, "Failed to start mlink service"
    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"
    result = mlink.wait_for_checkpoint()
    assert result is True, "Mongolink failed to save checkpoint"
    src["test_db1"].test_collection.insert_one({"a": {"b": []}, "words": "omnibus"})
    src["test_db1"].test_collection.delete_one({"a.b": [], "words": "omnibus"})
    src["test_db1"].test_collection.create_index([("a.b", 1), ("words", "text")],name="my_custom_index1")
    result = mlink.restart()
    assert result is True, "Failed to restart mlink service"
    result = mlink.wait_for_zero_lag()
    if not result:
        assert "text index contains an array in document" in mlink.logs()
        pytest.xfail("Known issue: PML-128")
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    expected_error = "detected concurrent process"
    if not mlink_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))