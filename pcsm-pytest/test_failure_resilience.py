import pytest
import pymongo
import time
import threading
import datetime
import re
from bson import ObjectId

from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data

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

@pytest.mark.jenkins
@pytest.mark.parametrize("cluster_configs", ["replicaset_3n", "sharded_3n"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
@pytest.mark.csync_log_level("trace")
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_csync_PML_T46(start_cluster, src_cluster, dst_cluster, csync, fail_node):
    """
    Test to check PCSM failure tolerance when SRC or DST primary goes down during clone stage
    """
    target = src_cluster if fail_node == "src" else dst_cluster
    try:
        is_sharded = src_cluster.layout == "sharded"
        generate_dummy_data(src_cluster.connection, is_sharded=is_sharded)
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
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
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=is_sharded)
        result = csync.wait_for_repl_stage()
        if not result and fail_node == "src":
            # PCSM doesn't tolerate SRC primary failure during clone stage
            csync.create(log_level="trace", extra_args="--reset-state")
            assert csync.start(), "Failed to restart csync service after repl_stage failure"
            assert csync.wait_for_repl_stage() is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
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
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.jenkins
@pytest.mark.parametrize("cluster_configs", ["replicaset_3n", "sharded_3n"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_csync_PML_T47(start_cluster, src_cluster, dst_cluster, csync, fail_node):
    """
    Test to check PCSM failure tolerance when SRC or DST primary goes down during replication stage
    """
    target = src_cluster if fail_node == "src" else dst_cluster
    try:
        is_sharded = src_cluster.layout == "sharded"
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.start() is True, "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.wait_for_repl_stage() is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db1", start_crud=True, is_sharded=is_sharded)
        _, operation_threads_4 = create_all_types_db(src_cluster.connection, "repl_test_db2", start_crud=True, is_sharded=is_sharded)
        target.restart_primary(5, force=False)
        _, operation_threads_5 = create_all_types_db(src_cluster.connection, "repl_test_db3", start_crud=True, is_sharded=is_sharded)
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
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.jenkins
@pytest.mark.parametrize("cluster_configs", ["replicaset_3n", "sharded_3n"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_csync_PML_T48(start_cluster, src_cluster, dst_cluster, csync, fail_node):
    """
    Test to check PCSM failure tolerance when SRC or DST primary steps down during replication stage
    """
    target = src_cluster if fail_node == "src" else dst_cluster
    stop_event = threading.Event()
    bg_threads = []
    try:
        is_sharded = src_cluster.layout == "sharded"
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        bg_threads += operation_threads_1
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        for i in range(5):
            db_name = f"repl_test_db_{i}"
            bg_threads.append(add_data(src_cluster.connection, db_name, stop_event))
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
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.jenkins
@pytest.mark.parametrize("cluster_configs", ["replicaset_3n", "sharded_3n"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_csync_PML_T49(start_cluster, src_cluster, dst_cluster, csync, fail_node):
    """
    Test to check PCSM failure tolerance when connection is lost to SRC or DST during replication stage
    """
    target = src_cluster if fail_node == "src" else dst_cluster
    stop_event = threading.Event()
    bg_threads = []
    try:
        is_sharded = src_cluster.layout == "sharded"
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        bg_threads += operation_threads_1
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        for i in range(5):
            db_name = f"repl_test_db_{i}"
            bg_threads.append(add_data(src_cluster.connection, db_name, stop_event))
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
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"