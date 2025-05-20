import pytest
import pymongo
import time
import threading
import docker
import re

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
def test_rs_mlink_PML_T9(reset_state, srcRS, dstRS, mlink):
    """
    Test to verify collection drop and re-creation during clone phase
    """
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    for i in range(5):
        src["dummy"].create_collection(f"collection_{i}", capped=True, size=2147483648, max=500000)
    generate_dummy_data(srcRS.connection, 'dummy', 5, 500000, drop_before_creation=False)
    for i in range(5):
        src["dummy"][f"collection_{i}"].create_index([("array", 1)])

    result = mlink.start()
    assert result is True, "Failed to start mlink service"

    for i in range(5):
        src["dummy"].command("collMod", f"collection_{i}", cappedSize=512 * 1024, cappedMax=500)
        src["dummy"][f"collection_{i}"].drop_indexes()
    for i in range(5):
        src["dummy"].drop_collection(f"collection_{i}")

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"

    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    expected_errors = ["NamespaceNotFound", "IndexNotFound", "collection not found", "No indexes to create"]
    if not mlink_error:
        unexpected = [line for line in error_logs if all(expected_error not in line for expected_error in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T10(reset_state, srcRS, dstRS, mlink):
    """
    Test to verify collection drop and re-creation replication phase
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"

        repl_test_db, _ = create_all_types_db(srcRS.connection, "repl_test_db", create_ts=True)

        # Re-create data during replication phase by dropping and re-creating the collections
        repl_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "repl_test_db",
                                                                drop_before_creation=True, start_crud=True)
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
def test_rs_mlink_PML_T11(reset_state, srcRS, dstRS, mlink):
    """
    Test to verify DB drop and re-creation during clone phase
    """
    try:
        mlink_env = {"PML_CLONE_NUM_PARALLEL_COLLECTIONS": "5"}
        mlink.create(log_level="trace", extra_args="--reset-state", env_vars=mlink_env)
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        generate_dummy_data(srcRS.connection, "init_test_db")

        # Re-create data during clone phase by dropping DB
        def start_mlink():
            result = mlink.start()
            assert result is True, "Failed to start mlink service"
        def delayed_drop():
            log_stream = mlink.logs(stream=True)
            pattern = re.compile(r'read batch.*ns=init_test_db\.collection_0.*s=copy')
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if pattern.search(line):
                    break
            src.drop_database("init_test_db")
        t1 = threading.Thread(target=start_mlink)
        t2 = threading.Thread(target=delayed_drop)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
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
    assert result is True, "Failed to catch up on replication"

    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    expected_errors = ["QueryPlanKilled","RetryableWrite"]
    if not mlink_error:
        unexpected = [line for line in error_logs if all(expected_error not in line for expected_error in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T12(reset_state, srcRS, dstRS, mlink):
    """
    Test to verify DB drop and re-creation during replication phase
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"

        repl_test_db, _ = create_all_types_db(srcRS.connection, "repl_test_db")

        # Re-create data during replication phase by dropping DB
        src.drop_database("repl_test_db")
        repl_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
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