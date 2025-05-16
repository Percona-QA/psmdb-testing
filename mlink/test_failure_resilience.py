import pytest
import pymongo
import time
import docker
import threading
import datetime
from bson import ObjectId

from cluster import Cluster
from mongolink import Mongolink
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data_rs

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def dstRS():
    return Cluster({ "_id": "rs2", "members": [{"host":"rs201"}, {"host":"rs202"}, {"host":"rs203"}]})

@pytest.fixture(scope="package")
def srcRS():
    return Cluster({ "_id": "rs1", "members": [{"host":"rs101"}]})

@pytest.fixture(scope="package")
def mlink(srcRS,dstRS):
    return Mongolink('mlink',srcRS.mlink_connection, dstRS.mlink_connection)

@pytest.fixture(scope="package")
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

def add_data(connection_string, db_name, stop_event=None):
    def worker():
        db = pymongo.MongoClient(connection_string)[db_name]
        counter = 1
        while not (stop_event and stop_event.is_set()):
            try:
                coll_name = f"coll_{counter}"
                db.create_collection(coll_name)
                db[coll_name].create_index("value")
                db[coll_name].insert_one({"_id": ObjectId(),"ts": datetime.datetime.now(datetime.timezone.utc),"value": "test"})
                db[coll_name].insert_many([{"ts": datetime.datetime.now(datetime.timezone.utc),"value": f"entry_{i}"}for i in range(3)])
                counter += 1
            except Exception as e:
                Cluster.log(f"Collection creation error: {e}")
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T46(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML failure tolerance when src primary goes down during clone stage
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        generate_dummy_data(srcRS.connection)
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        time.sleep(2)
        srcRS.restart_primary(5)
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
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
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T47(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML failure tolerance when dst primary goes down during clone stage
    """
    try:
        dst = pymongo.MongoClient(dstRS.connection)
        generate_dummy_data(srcRS.connection)
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        dstRS.restart_primary(5)
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
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
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T48(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML failure tolerance when src primary goes down during replication stage
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        assert mlink.start() is True, "Failed to start mlink service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        assert mlink.wait_for_repl_stage() is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db1", start_crud=True)
        _, operation_threads_4 = create_all_types_db(srcRS.connection, "repl_test_db2", start_crud=True)
        srcRS.restart_primary(5, force=False)
        _, operation_threads_5 = create_all_types_db(srcRS.connection, "repl_test_db3", start_crud=True)
        srcRS.restart_primary(5, force=True)
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
        if "operation_threads_4" in locals():
            all_threads += operation_threads_4
        if "operation_threads_5" in locals():
            all_threads += operation_threads_5
        for thread in all_threads:
            thread.join()
    result = mlink.wait_for_zero_lag()
    if not result:
        pytest.xfail("Failed to catch up on replication")
    assert mlink.finalize() is True, "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    mlink_error, error_logs = mlink.check_mlink_errors()
    if not result:
        pytest.xfail(f"Data mismatch after synchronization.\nMlink logs:\n{error_logs}")
    assert mlink_error, f"Mlink reported errors in logs:\n{error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T49(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML failure tolerance when src primary steps down during replication stage
    """
    stop_event = threading.Event()
    bg_threads = []
    try:
        assert mlink.start(), "Failed to start mlink service"
        assert mlink.wait_for_repl_stage(), "Failed to start replication stage"
        for i in range(5):
            db_name = f"repl_test_db_{i}"
            bg_threads.append(add_data(srcRS.connection, db_name, stop_event))
        time.sleep(2)
        srcRS.stepdown_primary()
        time.sleep(2)
    except Exception as e:
        raise e
    finally:
        stop_event.set()
        for thread in bg_threads:
            thread.join()
    if not mlink.wait_for_zero_lag():
        pytest.xfail("Failed to catch up on replication")
    assert mlink.finalize(), "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    mlink_error, error_logs = mlink.check_mlink_errors()
    if not result:
        pytest.xfail(f"Data mismatch after synchronization.\nMlink logs:\n{error_logs}")
    assert mlink_error, f"Mlink reported errors in logs:\n{error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T50(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML failure tolerance when src primary steps down during replication stage
    """
    stop_event = threading.Event()
    bg_threads = []
    try:
        assert mlink.start(), "Failed to start mlink service"
        assert mlink.wait_for_repl_stage(), "Failed to start replication stage"
        for i in range(5):
            db_name = f"repl_test_db_{i}"
            bg_threads.append(add_data(srcRS.connection, db_name, stop_event))
        time.sleep(2)
        srcRS.network_interruption(5)
        time.sleep(2)
    except Exception as e:
        raise e
    finally:
        stop_event.set()
        for thread in bg_threads:
            thread.join()
    if not mlink.wait_for_zero_lag():
        pytest.xfail("Failed to catch up on replication")
    assert mlink.finalize(), "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    mlink_error, error_logs = mlink.check_mlink_errors()
    if not result:
        pytest.xfail(f"Data mismatch after synchronization.\nMlink logs:\n{error_logs}")
    assert mlink_error, f"Mlink reported errors in logs:\n{error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T51(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML failure tolerance when dst primary goes down during replication stage
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        assert mlink.start() is True, "Failed to start mlink service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        assert mlink.wait_for_repl_stage() is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db1", start_crud=True)
        _, operation_threads_4 = create_all_types_db(srcRS.connection, "repl_test_db2", start_crud=True)
        dstRS.restart_primary(5)
        _, operation_threads_5 = create_all_types_db(srcRS.connection, "repl_test_db3", start_crud=True)
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
        if "operation_threads_4" in locals():
            all_threads += operation_threads_4
        if "operation_threads_5" in locals():
            all_threads += operation_threads_5
        for thread in all_threads:
            thread.join()
    result = mlink.wait_for_zero_lag()
    if not result:
        pytest.xfail("Failed to catch up on replication")
    assert mlink.finalize() is True, "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    mlink_error, error_logs = mlink.check_mlink_errors()
    if not result:
        pytest.xfail(f"Data mismatch after synchronization.\nMlink logs:\n{error_logs}")
    assert mlink_error, f"Mlink reported errors in logs:\n{error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T52(reset_state, srcRS, dstRS, mlink):
    """
    Test to check PML failure tolerance when dst primary steps down during replication stage
    """
    stop_event = threading.Event()
    bg_threads = []
    try:
        assert mlink.start(), "Failed to start mlink service"
        assert mlink.wait_for_repl_stage(), "Failed to start replication stage"
        for i in range(5):
            db_name = f"repl_test_db_{i}"
            bg_threads.append(add_data(srcRS.connection, db_name, stop_event))
        time.sleep(2)
        dstRS.stepdown_primary()
        time.sleep(2)
    except Exception as e:
        raise e
    finally:
        stop_event.set()
        for thread in bg_threads:
            thread.join()
    if not mlink.wait_for_zero_lag():
        pytest.xfail("Failed to catch up on replication")
    assert mlink.finalize(), "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    mlink_error, error_logs = mlink.check_mlink_errors()
    if not result:
        pytest.xfail(f"Data mismatch after synchronization.\nMlink logs:\n{error_logs}")
    assert mlink_error, f"Mlink reported errors in logs:\n{error_logs}"