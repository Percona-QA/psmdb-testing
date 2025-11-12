import pytest
import pymongo
import time
import threading
import re
import pymongo.errors

from cluster import Cluster
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T2(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate basic sync of all data types including time-series. Data is added before sync,
    during data clone and replication phase, also CRUD operations are performed all time during the sync.
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"
        # Add data before sync
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", create_ts=True, start_crud=True, is_sharded=is_sharded)
        assert csync.start(), "Failed to start csync service"
        # Add data during clone phase
        clone_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", create_ts=True, start_crud=True, is_sharded=is_sharded)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        # Add data during replication phase
        repl_test_db, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", create_ts=True, start_crud=True, is_sharded=is_sharded)
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
    # This step is required to ensure that all data is synchronized except for time-series
    # collections which are not supported. Existence of TS collections in source cluster
    # will cause the comparison to fail, but collections existence is important to verify
    # that csync can ignore time-series collections and all related events
    databases = ["init_test_db", "clone_test_db", "repl_test_db"]
    for db in databases:
        src[db].drop_collection("timeseries_data")
        src[db].drop_collection("sharded_timeseries_collection")

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T3(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate handling of index creation failures during clone and replication phase due to
    IndexOptionsConflict error (index with the same key spec already exists with a different name). The failed index will be created during the finalization stage.
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        init_test_db.invalid_index_collection.insert_many([
            {"first_name": "Alice", "last_name": "Smith", "age": 30},
            {"first_name": "Bob", "last_name": "Brown", "age": 25}])
        init_test_db.invalid_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_index")
        init_test_db.invalid_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_unique_index", unique=True)
        init_test_db.invalid_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True)

        assert csync.start(), "Failed to start csync service"

        clone_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=is_sharded)
        clone_test_db.invalid_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_index")
        clone_test_db.invalid_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_unique_index", unique=True)
        clone_test_db.invalid_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True)

        assert csync.wait_for_repl_stage(), "Failed to start replication stage"

        repl_test_db, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
        repl_test_db.invalid_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_index")
        repl_test_db.invalid_index_collection.create_index(
            [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
            name="compound_test_unique_index", unique=True)
        repl_test_db.invalid_index_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_test_sparse_index", sparse=True)
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
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "ERR One or more indexes failed to create"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))
    assert len(error_logs) == 3

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T4(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate handling of index creation failures during clone and replication phase due to
    IndexKeySpecsConflict error (existing index has the same name as the requested index but different key spec)
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        src["init_test_db"].duplicate_index_collection.insert_many([
            {"first_name": "Alice", "last_name": "Smith", "age": 30},
            {"first_name": "Bob", "last_name": "Brown", "age": 25}])

        assert csync.start(), "Failed to start csync service"

        def wait_for_collection(client, db_name, collection_name, timeout_sec=10, poll_interval=0.5):
            timeout = time.time() + timeout_sec
            while time.time() < timeout:
                if collection_name in client[db_name].list_collection_names():
                    return True
                time.sleep(poll_interval)
            return False
        assert wait_for_collection(dst, "init_test_db", "duplicate_index_collection"), \
            "Collection 'duplicate_index_collection' was not replicated to dst in time"
        dst["init_test_db"].duplicate_index_collection.create_index([("age", pymongo.ASCENDING)], name="conflict_index")
        src["init_test_db"].duplicate_index_collection.create_index([("name", pymongo.ASCENDING)], name="conflict_index")

        assert csync.wait_for_repl_stage(), "Failed to start replication stage"

        repl_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
        repl_test_db.duplicate_index_collection.insert_many([
            {"first_name": "Alice", "last_name": "Smith", "age": 30},
            {"first_name": "Bob", "last_name": "Brown", "age": 25}])
        assert wait_for_collection(dst, "repl_test_db", "duplicate_index_collection"), \
            "Collection 'duplicate_index_collection' was not replicated to dst in time"
        dst["repl_test_db"].duplicate_index_collection.create_index([("age", pymongo.ASCENDING)], name="conflict_index")
        src["repl_test_db"].duplicate_index_collection.create_index([("name", pymongo.ASCENDING)], name="conflict_index")
    except Exception:
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
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    result, summary = compare_data(src_cluster, dst_cluster)
    if not result:
        expected_mismatches = [
            ("init_test_db.duplicate_index_collection", "conflict_index"),
            ("repl_test_db.duplicate_index_collection", "conflict_index")]
        missing_mismatches = [index for index in expected_mismatches if index not in summary]
        unexpected_mismatches = [mismatch for mismatch in summary if mismatch not in expected_mismatches]
        assert not missing_mismatches, f"Expected mismatches missing: {missing_mismatches}"
        if unexpected_mismatches:
            pytest.fail("Unexpected mismatches:\n" + "\n".join(unexpected_mismatches))

    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "ERR One or more indexes failed to create"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T5(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate handling of index build failures during clone and replication phase due to
    - CannotBuildIndexKeys (index build failed)
    - DuplicateKey (index build failed)
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        init_test_db.invalid_text_collection1.insert_one({"a": {"b": []}, "words": "omnibus"})
        init_test_db.invalid_text_collection2.insert_one({"a": 1, "words": "omnibus"})
        init_test_db.invalid_unique_collection.insert_many([{"name": 1},{"x": 2},{"x": 3},{"x": 3}])

        assert csync.start(), "Failed to start csync service"
        time.sleep(1)

        def wait_for_collection(client, db_name, collection_name, timeout_sec=10, poll_interval=0.5):
            timeout = time.time() + timeout_sec
            while time.time() < timeout:
                if collection_name in client[db_name].list_collection_names():
                    return True
                time.sleep(poll_interval)
            return False
        # Check index build failure on src
        try:
            init_test_db.invalid_text_collection1.create_index([("a.b", 1), ("words", "text")])
            assert False, "Index build should fail due array in doc for text index"
        except pymongo.errors.OperationFailure as e:
            assert "text index contains an array" in str(e), f"Unexpected error: {e}"
        try:
            init_test_db.invalid_unique_collection.create_index("x", unique=True)
            assert False, "Index build should fail due to duplicate values"
        except pymongo.errors.OperationFailure as e:
            assert e.code == 11000 or "duplicate" in str(e), f"Unexpected error: {e}"
        # Check index build failure on dst
        assert wait_for_collection(dst, "init_test_db", "invalid_text_collection2"), \
            "Collection 'invalid_text_collection2' was not replicated to dst in time"
        dst["init_test_db"].invalid_text_collection2.insert_one({"a": {"b": []}, "words": "omnibus_new"})
        init_test_db.invalid_text_collection2.create_index([("a.b", 1), ("words", "text")])

        assert csync.wait_for_repl_stage(), "Failed to start replication stage"

        repl_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
        repl_test_db.invalid_text_collection1.insert_one({"a": {"b": []}, "words": "omnibus"})
        repl_test_db.invalid_text_collection2.insert_one({"a": 1, "words": "omnibus"})
        repl_test_db.invalid_unique_collection.insert_many([{"name": 1},{"x": 2},{"x": 3},{"x": 3}])
        # Check index build failure on src
        try:
            repl_test_db.invalid_text_collection1.create_index([("a.b", 1), ("words", "text")])
            assert False, "Index build should fail due array in doc for text index"
        except pymongo.errors.OperationFailure as e:
            assert "text index contains an array" in str(e), f"Unexpected error: {e}"
        try:
            repl_test_db.invalid_unique_collection.create_index("x", unique=True)
            assert False, "Index build should fail due to duplicate values"
        except pymongo.errors.OperationFailure as e:
            assert e.code == 11000 or "duplicate" in str(e), f"Unexpected error: {e}"
        # Check index build failure on dst
        assert wait_for_collection(dst, "repl_test_db", "invalid_text_collection2"), \
            "Collection 'invalid_text_collection2' was not replicated to dst in time"
        dst["repl_test_db"].invalid_text_collection2.insert_one({"a": {"b": []}, "words": "omnibus_new"})
        repl_test_db.invalid_text_collection2.create_index([("a.b", 1), ("words", "text")])
    except Exception:
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

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    # Remove manually added documents to dst collections
    dst["init_test_db"].invalid_text_collection2.delete_one({"a": {"b": []}, "words": "omnibus_new"})
    dst["repl_test_db"].invalid_text_collection2.delete_one({"a": {"b": []}, "words": "omnibus_new"})
    assert csync.finalize(), "Failed to finalize csync service"

    result, summary = compare_data(src_cluster, dst_cluster)
    if not result:
        expected_mismatches = [
            ("init_test_db.invalid_text_collection2", "a.b_1_words_text"),
            ("repl_test_db.invalid_text_collection2", "a.b_1_words_text")]
        missing_mismatches = [index for index in expected_mismatches if index not in summary]
        unexpected_mismatches = [mismatch for mismatch in summary if mismatch not in expected_mismatches]
        assert not missing_mismatches, f"Expected mismatches missing: {missing_mismatches}"
        if unexpected_mismatches:
            pytest.fail("Unexpected mismatches:\n" + "\n".join(unexpected_mismatches))

    csync_error, error_logs = csync.check_csync_errors()
    expected_errors = ["ERR One or more indexes failed to create", "ERR No incomplete indexes to add"]
    if not csync_error:
        unexpected = [line for line in error_logs if all(expected_error not in line for expected_error in expected_errors)]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T6(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate handling of index build failures during clone and replication phase due to IndexBuildAborted error
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"

        generate_dummy_data(src_cluster.connection, "dummy", 5, 300000, is_sharded=is_sharded)
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)

        assert csync.start(), "Failed to start csync service"
        time.sleep(1)

        index_spec = [("array", 1), ("padding1", 1), ("padding2", 1)]
        collection = src["dummy"]["collection_0"]
        def create_heavy_index():
            try:
                collection.create_index(index_spec)
            except pymongo.errors.PyMongoError as e:
                assert "IndexBuildAborted" in str(e), f"Unexpected error: {e}"
                Cluster.log(f"Index build was aborted: {e}")
        def drop_heavy_index():
            time.sleep(0.05)
            try:
                collection.drop_index(index_spec)
            except pymongo.errors.PyMongoError as e:
                Cluster.log(f"Drop index failed (not yet created or already dropped): {e}")
        create = threading.Thread(target=create_heavy_index)
        drop = threading.Thread(target=drop_heavy_index)
        create.start()
        drop.start()
        create.join()
        drop.join()

        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        repl_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)

        index_spec = [("array", 1), ("padding1", 1), ("padding2", 1)]
        collection = src["dummy"]["collection_1"]
        def create_heavy_index():
            try:
                collection.create_index(index_spec)
            except pymongo.errors.PyMongoError as e:
                assert "IndexBuildAborted" in str(e), f"Unexpected error: {e}"
                Cluster.log(f"Index build was aborted: {e}")
        def drop_heavy_index():
            time.sleep(0.05)
            try:
                collection.drop_index(index_spec)
            except pymongo.errors.PyMongoError as e:
                Cluster.log(f"Drop index failed (not yet created or already dropped): {e}")
        create = threading.Thread(target=create_heavy_index)
        drop = threading.Thread(target=drop_heavy_index)
        create.start()
        drop.start()
        create.join()
        drop.join()

    except Exception:
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

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T7(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate handling of unique index conversion failure due to CannotConvertIndexToUnique error during
    finalize phase. Index conversion should fail, however all other indexes should be converted successfully.
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"

        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        db_name = "test_db"
        coll_name1, coll_name2 = "test_collection1", "test_collection2"
        src[db_name][coll_name1].insert_many([{"name": 1},{"x": 2},{"x": 3}])
        src[db_name][coll_name2].insert_many([{"name": 1},{"x": 2},{"x": 3}])

        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"

        repl_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)

        src[db_name][coll_name1].create_index("x", unique=True)
        assert True, "Index creation should succeed"
        # Add duplicate record to dst to force failure on finalize stage
        res_doc = dst[db_name][coll_name1].insert_one({"x": 3})
        src[db_name][coll_name2].create_index("x", unique=True)
        assert True, "Index creation should succeed"

    except Exception:
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

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    # Remove manually added documents to dst collections
    dst[db_name][coll_name1].delete_one({"_id": res_doc.inserted_id})

    expected_mismatches = [("test_db.test_collection1", "x_1")]
    result, summary = compare_data(src_cluster, dst_cluster)
    if not result:
        missing_mismatches = [index for index in expected_mismatches if index not in summary]
        unexpected_mismatches = [mismatch for mismatch in summary if mismatch not in expected_mismatches]
        assert not missing_mismatches, f"Expected mismatches missing: {missing_mismatches}"
        if unexpected_mismatches:
            pytest.fail("Unexpected mismatches:\n" + "\n".join(f"{m}" for m in unexpected_mismatches))

    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "convert to unique"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T8(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate handling of collection existence on dst during clone and replication phase
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"

        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        dst["test_db1"].create_collection("duplicate_collection", collation={"locale": "en","strength": 2})
        dst["test_db1"].duplicate_collection.insert_one({"_id": "1", "field": "1"})
        src["test_db1"].create_collection("duplicate_collection", capped=True, size=1024 * 1024, max=20)
        src["test_db1"].duplicate_collection.insert_one({"_id": "1", "field": "2"})

        assert csync.start(), "Failed to start csync service"

        dst["test_db2"].create_collection("duplicate_collection", collation={"locale": "en","strength": 2})
        dst["test_db2"].duplicate_collection.insert_one({"_id": "1", "field": "1"})
        src["test_db2"].create_collection("duplicate_collection", capped=True, size=1024 * 1024, max=20)
        src["test_db2"].duplicate_collection.insert_one({"_id": "1", "field": "2"})

        assert csync.wait_for_repl_stage(), "Failed to start replication stage"

        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
        dst["test_db3"].create_collection("duplicate_collection", collation={"locale": "en","strength": 2})
        dst["test_db3"].duplicate_collection.insert_one({"_id": "1", "field": "1"})
        src["test_db3"].create_collection("duplicate_collection", capped=True, size=1024 * 1024, max=20)
        src["test_db3"].duplicate_collection.insert_one({"_id": "1", "field": "2"})

    except Exception:
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

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"

    assert csync.finalize(), "Failed to finalize csync service"

    for db_name in ["test_db1", "test_db2", "test_db3"]:
        assert "duplicate_collection" in dst[db_name].list_collection_names(), \
            f"'duplicate_collection' not found in {db_name} on destination"
        stats = dst[db_name].command("collstats", "duplicate_collection")
        assert stats.get("capped") is True, \
            f"'duplicate_collection' in {db_name} is not capped on destination"
        doc = dst[db_name]["duplicate_collection"].find_one({"_id": "1"})
        assert doc is not None, \
            f"Expected doc is missing from 'duplicate_collection' in {db_name}"
        assert doc["field"] == "2", \
            f"Doc in {db_name}.duplicate_collection was not properly overwritten"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "5"})
@pytest.mark.csync_log_level("trace")
def test_csync_PML_T30(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate handling of concurrent data clone and index build failure
    """
    src = pymongo.MongoClient(src_cluster.connection)
    normal_docs = [{"a": {"b": 1}, "words": "omnibus"} for _ in range(20000)]
    src["init_test_db"].invalid_text_collection1.insert_many(normal_docs)
    src["init_test_db"].invalid_text_collection1.insert_one({"a": {"b": []}, "words": "omnibus"})
    def start_csync():
        assert csync.start(), "Failed to start csync service"
    def invalid_index_creation():
        try:
            log_stream = csync.logs(stream=True)
            pattern = re.compile(r'Dropped collection')
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if pattern.search(line):
                    break
            src["init_test_db"].invalid_text_collection1.create_index([("a.b", 1), ("words", "text")])
            assert False, "Index build should fail due array in doc for text index"
        except pymongo.errors.OperationFailure as e:
            assert "text index contains an array" in str(e), f"Unexpected error: {e}"
    t1 = threading.Thread(target=start_csync)
    t2 = threading.Thread(target=invalid_index_creation)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert csync.wait_for_repl_stage(timeout=90) is True, "Failed to start replication stage"
    assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
    assert csync.finalize() is True, "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "ERR No incomplete indexes to add"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))


@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(600,func_only=True)
@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "200"})
@pytest.mark.csync_log_level("info")
def test_csync_PML_T31(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test how pcsm deals with huge number of namespaces on the clone phase
    """
    databases = 1000
    collections = 10
    Cluster.log("Creating " + str(databases) + " databases with " + str(collections) + " collections")
    client=pymongo.MongoClient(src_cluster.connection)
    for i in range(databases):
        db = 'test' + str(i)
        for j in range(collections):
            coll = 'test' + str(j)
            client[db][coll].insert_one({})
        Cluster.log("Created " + db)
    csync.start()
    result = csync.wait_for_repl_stage(500,10)
    assert result is True, "Failed to catch up on replication, csync logs:\n" + str(csync.logs(20, False))
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service, csync logs:\n" + str(csync.logs(20, False))
    client=pymongo.MongoClient(dst_cluster.connection)
    database_names = client.list_database_names()
    for i in range(databases):
        assert 'test' + str(i) in database_names, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(600,func_only=True)
def test_csync_PML_T43(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test how pcsm deals with huge number of indexes on the clone phase
    """
    collections = 200
    indexes = 50
    Cluster.log("Creating " + str(collections) + " collections with " + str(indexes) + " indexes")
    client=pymongo.MongoClient(src_cluster.connection)
    for i in range(collections):
        coll = 'test' + str(i)
        for j in range(indexes):
            index = 'test' + str(j)
            client['test'][coll].create_index(index)
        client['test'][coll].insert_one({})
        Cluster.log("Created " + coll)
    csync.start()
    result = csync.wait_for_repl_stage(300,10)
    assert result is True, "Failed to catch up on replication, csync logs:\n" + str(csync.logs(20))
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service, csync logs:\n" + str(csync.logs(20))
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"