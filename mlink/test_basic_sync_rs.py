import pytest
import pymongo
import time
import docker

from cluster import Cluster
from mongolink import Mongolink
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data_rs
from metrics_collector import metrics_collector

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def dstRS():
    return Cluster({ "_id": "rs2", "members": [{"host":"rs201"}]})

@pytest.fixture(scope="package")
def srcRS():
    return Cluster({ "_id": "rs1", "members": [{"host":"rs101"}]})

@pytest.fixture(scope="package")
def mlink(srcRS,dstRS):
    return Mongolink('mlink',srcRS.mlink_connection, dstRS.mlink_connection)

@pytest.fixture(scope="function")
def start_cluster(srcRS, dstRS, mlink, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        mlink.destroy()
        srcRS.create()
        dstRS.create()
        mlink.create()
        yield True

    finally:
        if request.config.getoption("--verbose"):
            logs = mlink.logs()
            print(f"\n\nmlink Last 50 Logs for mlink:\n{logs}\n\n")
        srcRS.destroy()
        dstRS.destroy()
        mlink.destroy()


def test_rs_mlink_PML_T2(start_cluster, srcRS, dstRS, mlink, metrics_collector):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    generate_dummy_data(srcRS.connection)
    # Add data before sync
    init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", create_ts=True, start_crud=True)

    result = mlink.start()
    assert result is True, "Failed to start mlink service"

    # Add data during clone phase
    clone_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", create_ts=True, start_crud=True)

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"

    # Add data during replication phase
    repl_test_db, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", create_ts=True, start_crud=True)

    time.sleep(10)

    stop_all_crud_operations()
    all_threads = operation_threads_1 + operation_threads_2 + operation_threads_3
    for thread in all_threads:
        thread.join()

    # This step is required to ensure that all data is synchronized except for time-series
    # collections which are not supported. Existence of TS collections in source cluster
    # will cause the comparison to fail, but collections existence is important to verify
    # that mlink can ignore time-series collections and all related events
    databases = ["init_test_db", "clone_test_db", "repl_test_db"]
    for db in databases:
        src[db].drop_collection("timeseries_data")

    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs:\n{chr(10).join(error_logs)}"


def disabled_rs_mlink_PML_T3(start_cluster, srcRS, dstRS, mlink):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    generate_dummy_data(srcRS.connection)
    init_test_db, _ = create_all_types_db(srcRS.connection, "init_test_db", create_ts=True)

    result = mlink.start()
    assert result is True, "Failed to start mlink service"

    # Re-create data during clone phase by dropping and re-creating the collections
    init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", create_ts=True, \
                                                            drop_before_creation=True, start_crud=True)

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"

    repl_test_db, _ = create_all_types_db(srcRS.connection, "repl_test_db", create_ts=True)

    # Re-create data during replication phase by dropping and re-creating the collections
    repl_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "repl_test_db", create_ts=True, \
                                                            drop_before_creation=True, start_crud=True)
    time.sleep(10)

    stop_all_crud_operations()
    all_threads = operation_threads_1 + operation_threads_2
    for thread in all_threads:
        thread.join()

    # This step is required to ensure that all data is synchronized except for time-series
    # collections which are not supported. Existence of TS collections in source cluster
    # will cause the comparison to fail, but collections existence is important to verify
    # that mlink can ignore time-series collections and all related events
    databases = ["init_test_db", "repl_test_db"]
    for db in databases:
        src[db].drop_collection("timeseries_data")

    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs:\n{chr(10).join(error_logs)}"

def disabled_rs_mlink_PML_T4(start_cluster, srcRS, dstRS, mlink):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    generate_dummy_data(srcRS.connection)
    init_test_db, _ = create_all_types_db(srcRS.connection, "init_test_db", create_ts=True)

    result = mlink.start()
    assert result is True, "Failed to start mlink service"

    # Re-create data during clone phase by dropping DB
    src.drop_database("init_test_db")
    init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", create_ts=True, start_crud=True)

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"

    repl_test_db, _ = create_all_types_db(srcRS.connection, "repl_test_db", create_ts=True)

    # Re-create data during replication phase by dropping DB
    src.drop_database("repl_test_db")
    repl_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "repl_test_db", create_ts=True, start_crud=True)
    time.sleep(10)

    stop_all_crud_operations()
    all_threads = operation_threads_1 + operation_threads_2
    for thread in all_threads:
        thread.join()

    # This step is required to ensure that all data is synchronized except for time-series
    # collections which are not supported. Existence of TS collections in source cluster
    # will cause the comparison to fail, but collections existence is important to verify
    # that mlink can ignore time-series collections and all related events
    databases = ["init_test_db", "repl_test_db"]
    for db in databases:
        src[db].drop_collection("timeseries_data")

    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs:\n{chr(10).join(error_logs)}"
