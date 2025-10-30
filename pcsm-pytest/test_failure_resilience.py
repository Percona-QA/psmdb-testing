import pytest
import pymongo
import time
import docker
import threading
import datetime
import re
from bson import ObjectId

from cluster import Cluster
from clustersync import Clustersync
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data_rs

@pytest.fixture(scope="module")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="module")
def dstRS():
    return Cluster({ "_id": "rs2", "members": [{"host":"rs201"}, {"host":"rs202"}, {"host":"rs203"}]})

@pytest.fixture(scope="module")
def srcRS():
    return Cluster({ "_id": "rs1", "members": [{"host":"rs101"}, {"host":"rs102"}, {"host":"rs103"}]})

@pytest.fixture(scope="module")
def csync(srcRS,dstRS):
    return Clustersync('csync',srcRS.csync_connection, dstRS.csync_connection)

@pytest.fixture(scope="module")
def start_cluster(srcRS, dstRS, csync, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        src_create_thread = threading.Thread(target=srcRS.create)
        dst_create_thread = threading.Thread(target=dstRS.create)
        src_create_thread.start()
        dst_create_thread.start()
        src_create_thread.join()
        dst_create_thread.join()
        yield True

    finally:
        srcRS.destroy()
        dstRS.destroy()
        csync.destroy()

@pytest.fixture(scope="function")
def reset_state(srcRS, dstRS, csync, request):
    log_level = "debug"
    env_vars = None
    log_marker = request.node.get_closest_marker("csync_log_level")
    if log_marker and log_marker.args:
        log_level = log_marker.args[0]
    env_marker = request.node.get_closest_marker("csync_env")
    if env_marker and env_marker.args:
        env_vars = env_marker.args[0]
    src_client = pymongo.MongoClient(srcRS.connection)
    dst_client = pymongo.MongoClient(dstRS.connection)
    def print_logs():
        if request.config.getoption("--verbose"):
            logs = csync.logs()
            print(f"\n\ncsync Last 50 Logs for csync:\n{logs}\n\n")
    request.addfinalizer(print_logs)
    csync.destroy()
    for db_name in src_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            src_client.drop_database(db_name)
    for db_name in dst_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            dst_client.drop_database(db_name)
    csync.create(log_level=log_level, env_vars=env_vars)

def add_data(connection_string, db_name, stop_event=None):
    def worker():
        db = pymongo.MongoClient(connection_string)[db_name]
        thread_id = threading.get_ident()
        counter = 1
        while not (stop_event and stop_event.is_set()):
            try:
                coll_name = f"coll_{counter}_{thread_id}"
                if coll_name not in db.list_collection_names():
                    db.create_collection(coll_name)
                collection = db[coll_name]
                collection.insert_one({"_id": ObjectId(), "ts": datetime.datetime.now(datetime.timezone.utc), "value": "test"})
                collection.insert_many([{"ts": datetime.datetime.now(datetime.timezone.utc), "value": f"entry_{i}"} for i in range(3)])
                counter += 1
            except pymongo.errors.PyMongoError as e:
                if "already exists" in str(e):
                    counter += 1
                else:
                    time.sleep(0.1)
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
@pytest.mark.csync_log_level("trace")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_rs_csync_PML_T46(reset_state, srcRS, dstRS, csync, fail_node):
    """
    Test to check PCSM failure tolerance when SRC or DST primary goes down during clone stage
    """
    target = srcRS if fail_node == "src" else dstRS
    try:
        generate_dummy_data(srcRS.connection)
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        def start_csync():
            assert csync.start() is True, "Failed to start csync service"
        def restart_primary():
            log_stream = csync.logs(stream=True)
            pattern = re.compile(r'read batch')
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if pattern.search(line):
                    break
            target.restart_primary(2, force=True)
        t1 = threading.Thread(target=start_csync)
        t2 = threading.Thread(target=restart_primary)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = csync.wait_for_repl_stage()
        if not result and fail_node == "src":
            # PCSM doesn't tolerate SRC primary failure during clone stage
            csync.create(log_level="trace", extra_args="--reset-state")
            assert csync.start(), "Failed to restart csync service after repl_stage failure"
            assert csync.wait_for_repl_stage() is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
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
    assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_rs_csync_PML_T47(reset_state, srcRS, dstRS, csync, fail_node):
    """
    Test to check PCSM failure tolerance when SRC or DST primary goes down during replication stage
    """
    target = srcRS if fail_node == "src" else dstRS
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        assert csync.start() is True, "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        assert csync.wait_for_repl_stage() is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db1", start_crud=True)
        _, operation_threads_4 = create_all_types_db(srcRS.connection, "repl_test_db2", start_crud=True)
        target.restart_primary(5, force=False)
        _, operation_threads_5 = create_all_types_db(srcRS.connection, "repl_test_db3", start_crud=True)
        target.restart_primary(5, force=True)
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
        if "operation_threads_4" in locals():
            all_threads += operation_threads_4
        if "operation_threads_5" in locals():
            all_threads += operation_threads_5
        for thread in all_threads:
            thread.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication after resuming from failure"
    assert csync.finalize(), "Failed to finalize csync service"
    time.sleep(5)
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_rs_csync_PML_T48(reset_state, srcRS, dstRS, csync, fail_node):
    """
    Test to check PCSM failure tolerance when SRC or DST primary steps down during replication stage
    """
    target = srcRS if fail_node == "src" else dstRS
    stop_event = threading.Event()
    bg_threads = []
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        bg_threads += operation_threads_1
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        for i in range(5):
            db_name = f"repl_test_db_{i}"
            bg_threads.append(add_data(srcRS.connection, db_name, stop_event))
        time.sleep(2)
        target.stepdown_primary()
        time.sleep(2)
    except Exception as e:
        raise e
    finally:
        stop_all_crud_operations()
        stop_event.set()
        for t in bg_threads:
            if t and t.is_alive():
                t.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication after resuming from failure"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_rs_csync_PML_T49(reset_state, srcRS, dstRS, csync, fail_node):
    """
    Test to check PCSM failure tolerance when connection is lost to SRC or DST during replication stage
    """
    target = srcRS if fail_node == "src" else dstRS
    stop_event = threading.Event()
    bg_threads = []
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        bg_threads += operation_threads_1
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        for i in range(5):
            db_name = f"repl_test_db_{i}"
            bg_threads.append(add_data(srcRS.connection, db_name, stop_event))
        time.sleep(2)
        target.network_interruption(10)
        time.sleep(2)
    except Exception as e:
        raise e
    finally:
        stop_all_crud_operations()
        stop_event.set()
        for t in bg_threads:
            if t and t.is_alive():
                t.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication after resuming from failure"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"