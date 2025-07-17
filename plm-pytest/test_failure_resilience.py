import pytest
import pymongo
import time
import docker
import threading
import datetime
import re
from bson import ObjectId

from cluster import Cluster
from perconalink import Perconalink
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
def plink(srcRS,dstRS):
    return Perconalink('plink',srcRS.plink_connection, dstRS.plink_connection)

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
    log_level = "debug"
    env_vars = None
    log_marker = request.node.get_closest_marker("plink_log_level")
    if log_marker and log_marker.args:
        log_level = log_marker.args[0]
    env_marker = request.node.get_closest_marker("plink_env")
    if env_marker and env_marker.args:
        env_vars = env_marker.args[0]
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
    plink.create(log_level=log_level, env_vars=env_vars)

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
@pytest.mark.plink_log_level("trace")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_rs_plink_PML_T46(reset_state, srcRS, dstRS, plink, fail_node):
    """
    Test to check PLM failure tolerance when SRC or DST primary goes down during clone stage
    """
    target = srcRS if fail_node == "src" else dstRS
    try:
        generate_dummy_data(srcRS.connection)
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        def start_plink():
            assert plink.start() is True, "Failed to start plink service"
        def restart_primary():
            log_stream = plink.logs(stream=True)
            pattern = re.compile(r'read batch')
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if pattern.search(line):
                    break
            target.restart_primary(2, force=True)
        t1 = threading.Thread(target=start_plink)
        t2 = threading.Thread(target=restart_primary)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = plink.wait_for_repl_stage()
        if not result and fail_node == "src":
            # PLM doesn't tolerate SRC primary failure during clone stage
            plink.create(log_level="trace", extra_args="--reset-state")
            assert plink.start(), "Failed to restart plink service after repl_stage failure"
            assert plink.wait_for_repl_stage() is True, "Failed to start replication stage"
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
    assert plink.wait_for_zero_lag() is True, "Failed to catch up on replication"
    assert plink.finalize(), "Failed to finalize plink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_rs_plink_PML_T47(reset_state, srcRS, dstRS, plink, fail_node):
    """
    Test to check PLM failure tolerance when SRC or DST primary goes down during replication stage
    """
    target = srcRS if fail_node == "src" else dstRS
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        assert plink.start() is True, "Failed to start plink service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        assert plink.wait_for_repl_stage() is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db1", start_crud=True)
        _, operation_threads_4 = create_all_types_db(srcRS.connection, "repl_test_db2", start_crud=True)
        target.restart_primary(5, force=False)
        _, operation_threads_5 = create_all_types_db(srcRS.connection, "repl_test_db3", start_crud=True)
        target.restart_primary(5, force=True)
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
    assert plink.wait_for_zero_lag(), "Failed to catch up on replication after resuming from failure"
    assert plink.finalize(), "Failed to finalize plink service"
    time.sleep(5)
    result, summary = compare_data_rs(srcRS, dstRS)
    if not result:
        critical_mismatches = {"hash mismatch", "record count mismatch", "missing in dst DB", "missing in src DB"}
        has_critical = any(mismatch[1] in critical_mismatches for mismatch in summary)
        if has_critical:
            pytest.fail("Critical mismatch found:\n" + "\n".join(str(m) for m in summary))
        else:
            pytest.xfail("Known issue: PLM-155")

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_rs_plink_PML_T48(reset_state, srcRS, dstRS, plink, fail_node):
    """
    Test to check PLM failure tolerance when SRC or DST primary steps down during replication stage
    """
    target = srcRS if fail_node == "src" else dstRS
    stop_event = threading.Event()
    bg_threads = []
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        bg_threads += operation_threads_1
        assert plink.start(), "Failed to start plink service"
        assert plink.wait_for_repl_stage(), "Failed to start replication stage"
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
    assert plink.wait_for_zero_lag(), "Failed to catch up on replication after resuming from failure"
    assert plink.finalize(), "Failed to finalize plink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_rs_plink_PML_T49(reset_state, srcRS, dstRS, plink, fail_node):
    """
    Test to check PLM failure tolerance when connection is lost to SRC or DST during replication stage
    """
    target = srcRS if fail_node == "src" else dstRS
    stop_event = threading.Event()
    bg_threads = []
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        bg_threads += operation_threads_1
        assert plink.start(), "Failed to start plink service"
        assert plink.wait_for_repl_stage(), "Failed to start replication stage"
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
    assert plink.wait_for_zero_lag(), "Failed to catch up on replication after resuming from failure"
    assert plink.finalize(), "Failed to finalize plink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"