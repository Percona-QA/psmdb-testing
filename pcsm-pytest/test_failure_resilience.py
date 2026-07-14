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
@pytest.mark.timeout(500,func_only=True)
@pytest.mark.csync_log_level("trace")
def test_csync_PML_T46(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM failure tolerance when DST primary goes down during clone stage.
    Note: SRC primary failure during clone stage is not checked here because PCSM doesn't
    tolerate it - sync fails and requires manual intervention with reset-state option
    """
    try:
        generate_dummy_data(src_cluster.connection, is_sharded=src_cluster.is_sharded)
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        def start_csync():
            assert csync.start() is True, "Failed to start csync service"
        def restart_primary():
            log_stream = csync.logs(stream=True)
            pattern = re.compile(r'read batch')
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if pattern.search(line):
                    break
            dst_cluster.restart_primary(2, force=True)
        t1 = threading.Thread(target=start_csync)
        t2 = threading.Thread(target=restart_primary)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.wait_for_repl_stage() is True, "Failed to start replication stage after DST primary restart"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
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
@pytest.mark.timeout(500,func_only=True)
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_csync_PML_T47(start_cluster, src_cluster, dst_cluster, csync, fail_node):
    """
    Test to check PCSM failure tolerance when SRC or DST primary goes down during replication stage
    """
    target = src_cluster if fail_node == "src" else dst_cluster
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start() is True, "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.wait_for_repl_stage() is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db1", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_4 = create_all_types_db(src_cluster.connection, "repl_test_db2", start_crud=True, is_sharded=src_cluster.is_sharded)
        target.restart_primary(5, force=False)
        _, operation_threads_5 = create_all_types_db(src_cluster.connection, "repl_test_db3", start_crud=True, is_sharded=src_cluster.is_sharded)
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
@pytest.mark.timeout(500,func_only=True)
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_csync_PML_T48(start_cluster, src_cluster, dst_cluster, csync, fail_node):
    """
    Test to check PCSM failure tolerance when SRC or DST primary steps down during replication stage
    """
    target = src_cluster if fail_node == "src" else dst_cluster
    stop_event = threading.Event()
    bg_threads = []
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
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
@pytest.mark.timeout(500,func_only=True)
@pytest.mark.parametrize("fail_node", ["src", "dst"])
def test_csync_PML_T49(start_cluster, src_cluster, dst_cluster, csync, fail_node):
    """
    Test to check PCSM failure tolerance when connection is lost to SRC or DST during replication stage
    """
    target = src_cluster if fail_node == "src" else dst_cluster
    stop_event = threading.Event()
    bg_threads = []
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        bg_threads += operation_threads_1
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        for i in range(5):
            db_name = f"repl_test_db_{i}"
            bg_threads.append(add_data(src_cluster.connection, db_name, stop_event))
        time.sleep(2)
        target.network_interruption(8)
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

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.timeout(500, func_only=True)
@pytest.mark.parametrize("scenario", [
    pytest.param(
        {"fail_command": "find", "skip": 0,
         "target": "getIDKeyRange ascending"},
        id="getIDKeyRange_asc",
    ),
    pytest.param(
        {"fail_command": "find", "skip": 1,
         "target": "getIDKeyRange descending"},
        id="getIDKeyRange_desc",
    ),
    pytest.param(
        {"fail_command": "find", "skip": 2,
         "target": "findSegmentMaxKey"},
        id="findSegmentMaxKey",
    ),
    pytest.param(
        {"fail_command": "find", "skip": 3,
         "target": "data cursor Find"},
        id="data_cursor_Find",
    ),
    pytest.param(
        {"fail_command": "getMore", "skip": 0,
         "target": "data cursor getMore"},
        id="data_cursor_getMore",
        marks=pytest.mark.xfail(strict=True,
            reason="data cursor getMore isn't retried, PCSM-328"))])
def test_csync_PML_T100(start_cluster, src_cluster, dst_cluster, csync, scenario):
    """
    Source-side transient-error matrix for clone
    Each scenario injects a single MaxTimeMSExpired on matching 
    command targeting the user namespace, then disables the failpoint. 
    PCSM is expected to recover and complete the clone successfully
    """
    namespace_db = "dummy"
    namespace_coll = "collection_0"
    namespace = f"{namespace_db}.{namespace_coll}"
    generate_dummy_data(src_cluster.connection, namespace_db,
                        num_collections=1, doc_size=25000, batch_size=10000)
    src = pymongo.MongoClient(src_cluster.connection)
    fp_data = {
        'failCommands': [scenario["fail_command"]],
        'namespace': namespace,
        'errorCode': 50,}
    skip = scenario["skip"]
    fp_mode = {'skip': skip} if skip > 0 else {'times': 1}
    src.admin.command({'configureFailPoint': 'failCommand',
                       'mode': fp_mode, 'data': fp_data})
    cancel_event = threading.Event()
    def _disable_failpoint_after(delay):
        # Returns True if cancelled (cancel_event set) before delay elapsed,
        # so failpoint disabling can be skipped if the test fails early
        if cancel_event.wait(delay):
            return
        try:
            src.admin.command({'configureFailPoint': 'failCommand', 'mode': 'off'})
        except Exception:
            pass
    disable_thread = threading.Thread(target=_disable_failpoint_after, args=(15,), daemon=True)
    try:
        assert csync.start(), "Failed to start csync service"
        disable_thread.start()
        assert csync.wait_for_repl_stage(), "Failed to start replication"
        assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
        assert csync.finalize(), "Failed to finalize csync service"
        result, _ = compare_data(src_cluster, dst_cluster)
        assert result is True, "Data mismatch after synchronization"
        csync_error, error_logs = csync.check_csync_errors()
        expected_errors = ["WRN Transient error"]
        if not csync_error:
            unexpected = [line for line in error_logs if all(expected_error not in line for expected_error in expected_errors)]
            if unexpected:
                pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))
    finally:
        cancel_event.set()
        disable_thread.join()
        src.admin.command({'configureFailPoint': 'failCommand', 'mode': 'off'})