import pytest
import pymongo
import time
import docker

from cluster import Cluster
from perconalink import Perconalink
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data_rs


@pytest.fixture(scope="module")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="module")
def dstRS():
    return Cluster({"_id": "rs2", "members": [{"host": "rs201"}]})


@pytest.fixture(scope="module")
def srcRS():
    return Cluster({"_id": "rs1", "members": [{"host": "rs101"}]})


@pytest.fixture(scope="module")
def plink(srcRS, dstRS):
    return Perconalink("plink", srcRS.plink_connection, dstRS.plink_connection)


@pytest.fixture(scope="module")
def start_cluster(srcRS, dstRS, plink, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        srcRS.create()
        dstRS.create()
        yield True

    finally:
        srcRS.destroy()
        dstRS.destroy()
        plink.destroy()


@pytest.fixture(scope="function")
def reset_state(srcRS, dstRS, plink, request):
    src_client = pymongo.MongoClient(srcRS.connection)
    dst_client = pymongo.MongoClient(dstRS.connection)

    def print_logs():
        if request.config.getoption("--verbose"):
            logs = plink.logs()
            print(f"\n\nplink Last 50 Logs for plink:\n{logs}\n\n")

    request.addfinalizer(print_logs)
    plink.destroy()
    for db_name in src_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            src_client.drop_database(db_name)
    for db_name in dst_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            dst_client.drop_database(db_name)
    plink.create()


@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T28(reset_state, srcRS, dstRS, plink):
    """
    Test to check PLM behavior when restarted during the clone and replication stages
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)
        generate_dummy_data(srcRS.connection)
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = plink.start()
        assert result is True, "Failed to start plink service"
        clone_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        # restart during clone stage
        plink.restart(reset=True)
        result = plink.start()
        assert result is True, "Failed to start plink service"
        result = plink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = plink.wait_for_checkpoint()
        assert result is True, "Perconalink failed to save checkpoint"
        repl_test_db, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        # restart during replication stage
        plink.restart()
        time.sleep(5)
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
    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    plink_error, error_logs = plink.check_plink_errors()
    expected_error = "detected concurrent process"
    if not plink_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))


@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T29(reset_state, srcRS, dstRS, plink):
    """
    Test to check PLM pause/resume options
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)
        generate_dummy_data(srcRS.connection)
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = plink.start()
        assert result is True, "Failed to start plink service"
        clone_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = plink.pause()
        assert result is False, "Can't pause plink service during clone stage"
        result = plink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = plink.pause()
        plink.restart()
        assert result is True, "Replication is paused"
        repl_test_db, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        time.sleep(5)
        result = plink.resume()
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
    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    plink_error, error_logs = plink.check_plink_errors()
    expected_errors = ["Change Replication is not runnning", "detected concurrent process"]
    if not plink_error:
        unexpected = [line for line in error_logs if not any(expected in line for expected in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))


@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T37(reset_state, srcRS, dstRS, plink):
    """
    Test to check PLM when it's not possible to resume due to lost oplog history
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)
        result = src.admin.command("replSetResizeOplog", size=990)
        assert result.get("ok") == 1.0, f"Failed to resize oplog: {result}"
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = plink.start()
        assert result is True, "Failed to start plink service"
        result = plink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = plink.pause()
        assert result is True, "Replication is paused"
        generate_dummy_data(srcRS.connection, "dummy", 15, 300000)
        result = plink.resume()
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
    result = plink.wait_for_zero_lag()
    if not result:
        assert "oplog history is lost" in plink.logs()
    status = plink.status()
    assert status["data"]["ok"] == False
    assert status["data"]["state"] != "running"


@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T38(reset_state, srcRS, dstRS, plink):
    """
    Test to check how PLM handles errors during recovery - if PLM "incorrectly" replays previously applied
    operations after the restart, the first insert will fail due to an existing index that prohibits such doc
    """
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)
    src["test_db1"].create_collection("test_collection")
    src["test_db2"].create_collection("test_collection")
    result = plink.start()
    assert result is True, "Failed to start plink service"
    result = plink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"
    result = plink.wait_for_checkpoint()
    assert result is True, "Perconalink failed to save checkpoint"
    src["test_db1"].test_collection.insert_one({"a": {"b": []}, "words": "omnibus"})
    src["test_db1"].test_collection.delete_one({"a.b": [], "words": "omnibus"})
    src["test_db1"].test_collection.create_index([("a.b", 1), ("words", "text")], name="my_custom_index1")
    result = plink.restart()
    assert result is True, "Failed to restart plink service"
    result = plink.wait_for_zero_lag()
    if not result:
        assert "text index contains an array in document" in plink.logs()
        pytest.xfail("Known issue: PLM-128")
    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    plink_error, error_logs = plink.check_plink_errors()
    expected_error = "detected concurrent process"
    if not plink_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))
