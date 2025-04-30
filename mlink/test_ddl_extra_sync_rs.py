import pytest
import pymongo
import time
import docker
import threading
import datetime
import re
from pymongo.errors import OperationFailure

from cluster import Cluster
from mongolink import Mongolink
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data_rs


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

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T13(reset_state, srcRS, dstRS, mlink):
    """
    Test collMod on collection with validator, validatorLevel, validatorAction
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        db_name = "test_db"
        coll_name = "test_collection"
        src[db_name].create_collection(
            coll_name,
            validator={"age": {"$gte": 18}},
            validationAction="warn",
            validationLevel="moderate"
        )
        src[db_name][coll_name].insert_one({"name": "Alice", "age": 30})

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        res = src[db_name].command(
            "collMod",
            coll_name,
            validator={"age": {"$gte": 21}},
            validationAction="error",
            validationLevel="strict"
        )
        assert res.get("ok") == 1.0, f"collMod failed: {res}"

    except Exception as e:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to finish init sync"
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
def test_rs_mlink_PML_T14(reset_state, srcRS, dstRS, mlink):
    """
    Test collMod on collection with changeStreamPreAndPostImages
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        db_name = "test_db"
        coll_name1 = "test_collection1"
        coll_name2 = "test_collection2"
        src[db_name].create_collection(coll_name1)
        src[db_name].create_collection(coll_name2, changeStreamPreAndPostImages={"enabled": True})
        src[db_name][coll_name1].insert_one({"name": "Alice", "age": 30})
        src[db_name][coll_name2].insert_one({"name": "Alice", "age": 30})

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        res = src[db_name].command(
            "collMod",
            coll_name1,
            changeStreamPreAndPostImages={"enabled": True})
        assert res.get("ok") == 1.0, f"collMod failed: {res}"

    except Exception as e:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to finish init sync"
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
def test_rs_mlink_PML_T15(reset_state, srcRS, dstRS, mlink):
    """
    Test collMod on view
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        db_name = "test_db"
        coll_name1, coll_name2 = "test_collection1", "test_collection2"
        view_name = "test_view"
        src[db_name][coll_name1].insert_many([
            {"user": "alice1", "status": "Q", "description": "Queued"},
            {"user": "bob1", "status": "D", "description": "Done"}])
        src[db_name][coll_name2].insert_many([
            {"user": "alice2", "status": "Q", "description": "Queued"},
            {"user": "bob2", "status": "D", "description": "Done"}])
        src[db_name].command({
            "create": view_name,
            "viewOn": coll_name1,
            "pipeline": [
                {"$match": {"status": "Q"}},
                {"$project": {"user": 1, "description": 1, "_id": 0}}]})

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        res = src[db_name].command(
        "collMod",
        view_name,
        viewOn=coll_name2,
        pipeline=[
            {"$match": {"status": "D"}},
            {"$project": {"user": 1, "description": 1, "_id": 0}}])
        assert res.get("ok") == 1.0, f"collMod failed on view: {res}"

    except Exception as e:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to finish init sync"
    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    dst_view_docs = list(dst[db_name][view_name].find({}))
    assert dst_view_docs == [{"user": "bob2", "description": "Done"}], f"View data mismatch: {dst_view_docs}"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T16(reset_state, srcRS, dstRS, mlink):
    """
    Test collMod on capped collection
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        db_name = "test_db"
        coll_name = "test_collection"
        src[db_name].create_collection(coll_name, capped=True, size=1024 * 1024, max=1000)
        src[db_name][coll_name].insert_one({"x": 1})

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        res = src[db_name].command(
        "collMod",
        coll_name,
        cappedSize=512 * 1024,
        cappedMax=500)
        assert res.get("ok") == 1.0, f"collMod failed: {res}"

    except Exception as e:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to finish init sync"
    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    cursor = list(dst[db_name].list_collections(filter={"name": coll_name}))
    assert cursor, f"Collection '{coll_name}' not found in destination"

    dst_opts = cursor[0].get("options", {})
    assert dst_opts.get("capped") is True
    assert dst_opts.get("size") <= 512 * 1024
    assert dst_opts.get("max") == 500

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T17(reset_state, srcRS, dstRS, mlink):
    """
    Test collMod on indexes: TTL, hidden, prepareUnique, unique, name, keyPattern
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        db_name = "test_db"
        coll_name = "test_collection"
        coll = src[db_name][coll_name]
        coll.insert_many([{"createdAt": datetime.datetime.now(datetime.timezone.utc), "a": i, "b": i, "c": -i, "d": i}
                          for i in range(5)])

        coll.create_index("createdAt", expireAfterSeconds=60)
        coll.create_index([("a", 1), ("b", -1)], name="ab_index")
        coll.create_index("c")
        coll.create_index("d")

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        # Modify TTL
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"createdAt": 1}, "expireAfterSeconds": 120})
        assert res.get("ok") == 1.0

        # Hide /unhide index by keyPattern / name
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"c": 1}, "hidden": True})
        assert res.get("ok") == 1.0
        res = src[db_name].command("collMod", coll_name, index={"name": "ab_index", "hidden": True})
        assert res.get("ok") == 1.0
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"c": 1}, "hidden": False})
        assert res.get("ok") == 1.0

        # PrepareUnique and dry run for unique
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"d": 1}, "prepareUnique": True})
        assert res.get("ok") == 1.0
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"d": 1}, "unique": True}, dryRun=True)
        assert res.get("ok") == 1.0

        # Prepare unique and convert to unique
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"c": 1}, "prepareUnique": True})
        assert res.get("ok") == 1.0
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"c": 1}, "unique": True})
        assert res.get("ok") == 1.0

    except Exception as e:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to finish init sync"
    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    time.sleep(1)
    index_docs = list(dst[db_name][coll_name].list_indexes())
    index_by_name = {idx["name"]: idx for idx in index_docs}
    assert index_by_name["createdAt_1"].get("expireAfterSeconds") == 120
    assert index_by_name["c_1"].get("unique") is True
    assert index_by_name["c_1"].get("hidden") is None
    assert index_by_name["ab_index"].get("hidden") is True
    assert index_by_name["d_1"].get("prepareUnique") is True

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T18(reset_state, srcRS, dstRS, mlink):
    """
    Test collmod when converting to unique fails
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        db_name = "test_db"
        coll_name = "test_collection"
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        src[db_name][coll_name].insert_many([
            {"x": 1},
            {"x": 1},
            {"x": 2}
        ])
        src[db_name][coll_name].create_index("x")

        result = mlink.start()
        assert result is True, "Failed to start mlink service"

        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"x": 1}, "prepareUnique": True})
        assert res.get("ok") == 1.0
        try:
            res = src[db_name].command(
                "collMod", coll_name,
                index={"keyPattern": {"x": 1}, "unique": True})
            assert False, "unique: true should fail due to duplicate values"
        except pymongo.errors.OperationFailure as e:
            code_name = e.details.get("codeName") if e.details else None
            assert code_name == "CannotConvertIndexToUnique", f"Unexpected codeName: {code_name}"

    except Exception as e:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    result = mlink.wait_for_repl_stage()
    assert result is True, "Failed to finish init sync"
    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    time.sleep(1)
    index_docs = list(dst[db_name][coll_name].list_indexes())
    idx = next(i for i in index_docs if i["name"] == "x_1")
    assert idx.get("unique") is not True, "Unique index should not be applied on destination"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T19(reset_state, srcRS, dstRS, mlink):
    """
    Test to check renameCollection while collection is being cloned
    """
    try:
        mlink_env = {
            "PML_CLONE_NUM_PARALLEL_COLLECTIONS": "5"
        }
        mlink.create(log_level="trace", extra_args="--reset-state", env_vars=mlink_env)
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        db_name = "test_db"
        old_name = "collection_4"
        new_name = "renamed_collection_4"
        generate_dummy_data(srcRS.connection, db_name, 5, 500000)

        def start_mlink():
            result = mlink.start()
            assert result is True, "Failed to start mlink service"
        def rename_collection():
            log_stream = mlink.logs(stream=True)
            pattern = re.compile(r'read batch \d+:\d+.*ns=test_db\.collection_4.*s=copy')
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if pattern.search(line):
                    break
            res = src.admin.command("renameCollection", f"{db_name}.{old_name}", to=f"{db_name}.{new_name}")
            assert res.get("ok") == 1.0, f"renameCollection failed: {res}"

        t1 = threading.Thread(target=start_mlink)
        t2 = threading.Thread(target=rename_collection)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    except Exception as e:
        raise

    result = mlink.wait_for_repl_stage(timeout=30)
    if not result:
        if "collection renamed" in mlink.logs(tail=2000):
                pytest.xfail("Known issue: PML-109")
        else:
            assert False, "Failed to start replication stage"

    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    time.sleep(1)
    dst_collections = dst[db_name].list_collection_names()
    assert new_name in dst_collections, "Renamed collection not found on destination"
    assert old_name not in dst_collections, "Old collection name still exists on destination"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"
    pytest.fail("Unexpected pass: test should have failed due to PML-109")

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T20(reset_state, srcRS, dstRS, mlink):
    """
    Test to check renameCollection during data clone
    """
    try:
        mlink_env = {
            "PML_CLONE_NUM_PARALLEL_COLLECTIONS": "5"
        }
        mlink.create(extra_args="--reset-state", env_vars=mlink_env)
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        generate_dummy_data(srcRS.connection, "dummy")

        db_name1, db_name2 = "test_db1", "test_db2"
        old_name1, old_name2, old_name3  = "test_collection1", "test_collection2", "test_collection3"
        new_name1, new_name2, new_name3 = "renamed_collection1", "renamed_collection2", "renamed_collection3"
        initial_docs_1 = [{"_id": i, "value": f"doc{i}", "email": f"user{i}@test.com", "field": i} for i in range(10)]
        initial_docs_2 = [{"_id": i, "value": f"doc{i}", "email_new": f"user{i}@test.com", "field": i} for i in range(10)]
        for coll_name in [old_name1, old_name2, old_name3]:
            src[db_name1][coll_name].insert_many(initial_docs_1)
        src[db_name1][new_name2].insert_many(initial_docs_2)

        def start_mlink():
            result = mlink.start()
            assert result is True, "Failed to start mlink service"
        def rename_collection():
            log_stream = mlink.logs(stream=True)
            watched_collections = {
                f"{db_name1}.{old_name1}": False,
                f"{db_name1}.{old_name2}": False,
                f"{db_name1}.{old_name3}": False,
            }
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if not watched_collections[f"{db_name1}.{old_name1}"] and f'Collection "{db_name1}.{old_name1}" created' in line:
                    res = src.admin.command("renameCollection", f"{db_name1}.{old_name1}", to=f"{db_name1}.{new_name1}")
                    assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
                    watched_collections[f"{db_name1}.{old_name1}"] = True
                elif not watched_collections[f"{db_name1}.{old_name2}"] and f'Collection "{db_name1}.{old_name2}" created' in line:
                    res = src.admin.command("renameCollection", f"{db_name1}.{old_name2}", to=f"{db_name1}.{new_name2}", dropTarget=True)
                    assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
                    watched_collections[f"{db_name1}.{old_name2}"] = True
                elif not watched_collections[f"{db_name1}.{old_name3}"] and f'Collection "{db_name1}.{old_name3}" created' in line:
                    res = src.admin.command("renameCollection", f"{db_name1}.{old_name3}", to=f"{db_name2}.{new_name3}")
                    assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
                    watched_collections[f"{db_name1}.{old_name3}"] = True
                if all(watched_collections.values()):
                    break

        t1 = threading.Thread(target=start_mlink)
        t2 = threading.Thread(target=rename_collection)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    except Exception as e:
        raise

    result = mlink.wait_for_repl_stage(timeout=30)
    if not result:
        if "collection renamed" in mlink.logs():
                pytest.xfail("Known issue: PML-110")
        else:
            assert False, "Failed to start replication stage"

    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    time.sleep(1)
    expected_db_mismatch = ("test_db1", "hash mismatch")
    expected_collections = ["test_db1.renamed_collection1", "test_db1.renamed_collection2"]
    expected_types = ["hash mismatch", "record count mismatch"]

    result, summary = compare_data_rs(srcRS, dstRS)
    if not result:
        summary_set = set(summary)
        if expected_db_mismatch not in summary_set:
            assert False, f"Expected mismatch missing: {expected_db_mismatch}"

        collections_with_both = []
        for coll in expected_collections:
            mismatches = [t for (c, t) in summary if c == coll]
            if all(m in mismatches for m in expected_types):
                collections_with_both.append(coll)
        if not collections_with_both:
            assert False, f"Expected at least one collection with both mismatches: {expected_collections}"

        allowed = [expected_db_mismatch]
        for coll in expected_collections:
            for mismatch_type in expected_types:
                allowed.append((coll, mismatch_type))
        unexpected = [m for m in summary if m not in allowed]
        if not unexpected:
            pytest.xfail("Known issue: PML-110")
        assert False, f"Unexpected mismatches found: {unexpected}"

    mlink_error, error_logs = mlink.check_mlink_errors()
    expected_error = "catalog:rename"
    if not mlink_error:
        has_expected = any(expected_error in line for line in error_logs)
        unexpected = [line for line in error_logs if expected_error not in line]

        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))
        elif has_expected:
            pytest.xfail(f"Expected fail: {expected_error}, no errors should be returned")
    else:
        pytest.fail("Unexpected pass: test should have failed due to PML-110")

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T21(reset_state, srcRS, dstRS, mlink):
    """
    Test to check renameCollection during repl stage
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        db_name1, db_name2 = "test_db1", "test_db2"
        old_name1, old_name2, old_name3  = "test_collection1", "test_collection2", "test_collection3"
        new_name1, new_name2, new_name3 = "renamed_collection1", "renamed_collection2", "renamed_collection3"
        initial_docs = [{"_id": i, "value": f"doc{i}", "email": f"user{i}@test.com", "field": i} for i in range(10)]
        collection_names = [old_name1, old_name2, old_name3, new_name2]
        for coll_name in collection_names:
            src[db_name1][coll_name].insert_many(initial_docs)
        src[db_name1][new_name2].create_index("email", unique=True, name="email_unique22")
        src[db_name1][old_name3].create_index("email", unique=True, name="email_unique31")

        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to finish init sync"

        repl_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        res = src.admin.command("renameCollection", f"{db_name1}.{old_name1}", to=f"{db_name1}.{new_name1}")
        assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
        res = src.admin.command("renameCollection", f"{db_name1}.{old_name2}", to=f"{db_name1}.{new_name2}", dropTarget=True)
        assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
        res = src.admin.command("renameCollection", f"{db_name1}.{old_name3}", to=f"{db_name2}.{new_name3}")
        assert res.get("ok") == 1.0, f"renameCollection failed: {res}"

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

    time.sleep(1)
    dst_collections1 = set(dst[db_name1].list_collection_names())
    dst_collections2 = set(dst[db_name2].list_collection_names())
    for name in [new_name1, new_name2]:
        assert name in dst_collections1, f"Expected collection '{name}' not found in {db_name1}"
    for name in [old_name1, old_name2, old_name3]:
        assert name not in dst_collections1, f"Old collection '{name}' still exists in {db_name1}"
    assert new_name3 in dst_collections2, f"Renamed collection '{new_name3}' not found in {db_name2}"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

    mlink_error, error_logs = mlink.check_mlink_errors()
    expected_error = "ERR add collection"
    if not mlink_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))