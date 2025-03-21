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


@pytest.mark.timeout(300,func_only=True)
def test_rs_mlink_PML_T2(start_cluster, srcRS, dstRS, mlink, metrics_collector):
    try:
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
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"

@pytest.mark.xfail(reason="Known issue: PML-84", strict=True)
@pytest.mark.timeout(300,func_only=True)
def test_rs_mlink_PML_T3(start_cluster, srcRS, dstRS, mlink):
    """
    Test to validate handling of index creation failures
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        # Add data before sync
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        init_test_db.failed_index_collection.insert_many([
            {"first_name": "Alice", "last_name": "Smith", "age": 30},
            {"first_name": "Bob", "last_name": "Brown", "age": 25}
        ])
        init_test_db.failed_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_index"
        )
        init_test_db.failed_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_unique_index", unique=True
        )
        init_test_db.failed_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True
        )

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        # Add data during clone phase
        clone_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        clone_test_db.failed_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_index"
        )
        clone_test_db.failed_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_unique_index", unique=True
        )
        clone_test_db.failed_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True
        )

        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"

        # Add data during replication phase
        repl_test_db, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        repl_test_db.failed_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_index"
        )
        repl_test_db.failed_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_unique_index", unique=True
        )
        repl_test_db.failed_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True
        )

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

    # Due to the fact that PML creates unique indexes as non-unique on the destination cluster, the creation
    # of the unique index will fail on the destination cluster with IndexOptionsConflict error. This is expected
    # behavior, however all other indexes should be created successfully.
    expected_mismatches = [
        ("init_test_db.failed_index_collection", "compound_test_unique_index"),
        ("clone_test_db.failed_index_collection", "compound_test_unique_index"),
        ("repl_test_db.failed_index_collection", "compound_test_unique_index")
    ]

    result, summary = compare_data_rs(srcRS, dstRS)
    assert result is False, "Data mismatch after synchronization"

    missing_mismatches = [index for index in expected_mismatches if index not in summary]
    unexpected_mismatches = [mismatch for mismatch in summary if mismatch not in expected_mismatches]

    assert not missing_mismatches, f"Expected mismatches missing: {missing_mismatches}"
    assert not unexpected_mismatches, f"Unexpected mismatches detected: {unexpected_mismatches}"

    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
def test_rs_mlink_PML_T4(start_cluster, srcRS, dstRS, mlink):
    """
    Test to validate handling of index duplicate errors
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        generate_dummy_data(srcRS.connection)
        # Add data before sync
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        init_test_db.duplicate_index_collection.insert_many([
            {"first_name": "Alice", "last_name": "Smith", "age": 30},
            {"first_name": "Bob", "last_name": "Brown", "age": 25}
        ])
        init_test_db.duplicate_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_index"
        )

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        # Add index to dst cluster to cause duplicate index error
        dst["init_test_db"].duplicate_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True
        )

        src["init_test_db"].duplicate_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True
        )

        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"

        # Add data during replication phase
        repl_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        repl_test_db.duplicate_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_index"
        )

        # Add index to dst cluster to cause duplicate index error
        dst["repl_test_db"].duplicate_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True
        )
        src["repl_test_db"].duplicate_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True
        )

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
