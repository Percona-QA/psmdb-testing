import pytest
import pymongo
import time
import threading
import datetime
import re

from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T13(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test collMod on collection with validator, validatorLevel, validatorAction
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        db_name = "test_db"
        coll_name = "test_collection"
        if src_cluster.is_sharded:
            src.admin.command({"enableSharding": db_name})
        src[db_name].create_collection(
            coll_name,
            validator={"age": {"$gte": 18}},
            validationAction="warn",
            validationLevel="moderate"
        )
        if src_cluster.is_sharded:
            src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"_id": "hashed"})
        src[db_name][coll_name].insert_many([{"name": f"Alice{i}", "age": 30 + i} for i in range(50)])
        assert csync.start(), "Failed to start csync service"

        res = src[db_name].command(
            "collMod",
            coll_name,
            validator={"age": {"$gte": 21}},
            validationAction="error",
            validationLevel="strict"
        )
        assert res.get("ok") == 1.0, f"collMod failed: {res}"
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    assert csync.wait_for_repl_stage(), "Failed to finish init sync"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T14(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test collMod on collection with changeStreamPreAndPostImages
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)

        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        db_name = "test_db"
        coll_name1 = "test_collection1"
        coll_name2 = "test_collection2"
        if src_cluster.is_sharded:
            src.admin.command({"enableSharding": db_name})
        src[db_name].create_collection(coll_name1)
        src[db_name].create_collection(coll_name2, changeStreamPreAndPostImages={"enabled": True})
        if src_cluster.is_sharded:
            src.admin.command("shardCollection", f"{db_name}.{coll_name1}", key={"_id": "hashed"})
            src.admin.command("shardCollection", f"{db_name}.{coll_name2}", key={"_id": "hashed"})
        src[db_name][coll_name1].insert_many([{"name": f"Alice{i}", "age": 30 + i} for i in range(50)])
        src[db_name][coll_name2].insert_many([{"name": f"Alice{i}", "age": 30 + i} for i in range(50)])
        assert csync.start(), "Failed to start csync service"

        res = src[db_name].command(
            "collMod",
            coll_name1,
            changeStreamPreAndPostImages={"enabled": True})
        assert res.get("ok") == 1.0, f"collMod failed: {res}"

    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    assert csync.wait_for_repl_stage(), "Failed to finish init sync"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T15(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test collMod on view
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)

        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
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

        assert csync.start(), "Failed to start csync service"

        res = src[db_name].command(
        "collMod",
        view_name,
        viewOn=coll_name2,
        pipeline=[
            {"$match": {"status": "D"}},
            {"$project": {"user": 1, "description": 1, "_id": 0}}])
        assert res.get("ok") == 1.0, f"collMod failed on view: {res}"

    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    assert csync.wait_for_repl_stage(), "Failed to finish init sync"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    dst_view_docs = list(dst[db_name][view_name].find({}))
    assert dst_view_docs == [{"user": "bob2", "description": "Done"}], f"View data mismatch: {dst_view_docs}"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T16(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test collMod on capped collection
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)

        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        db_name = "test_db"
        coll_name = "test_collection"
        src[db_name].create_collection(coll_name, capped=True, size=1024 * 1024, max=1000)
        src[db_name][coll_name].insert_one({"x": 1})

        assert csync.start(), "Failed to start csync service"

        res = src[db_name].command(
        "collMod",
        coll_name,
        cappedSize=512 * 1024,
        cappedMax=500)
        assert res.get("ok") == 1.0, f"collMod failed: {res}"

    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    assert csync.wait_for_repl_stage(), "Failed to finish init sync"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    cursor = list(dst[db_name].list_collections(filter={"name": coll_name}))
    assert cursor, f"Collection '{coll_name}' not found in destination"

    dst_opts = cursor[0].get("options", {})
    assert dst_opts.get("capped") is True
    assert dst_opts.get("size") <= 512 * 1024
    assert dst_opts.get("max") == 500

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T17(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test collMod on indexes: TTL, hidden, prepareUnique, unique, name, keyPattern
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)

        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        db_name = "test_db"
        coll_name = "test_collection"
        coll = src[db_name][coll_name]
        if src_cluster.is_sharded:
            src.admin.command({"enableSharding": db_name})
            src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"c": 1})
        coll.insert_many([{"createdAt": datetime.datetime.now(datetime.timezone.utc), "a": i, "b": i, "c": -i, "d": i}
                          for i in range(5)])

        coll.create_index("createdAt", expireAfterSeconds=60)
        coll.create_index([("a", 1), ("b", -1)], name="ab_index")
        coll.create_index("c")
        coll.create_index("d")
        coll.create_index([("c", 1), ("d", 1)], name="cd_index")

        assert csync.start(), "Failed to start csync service"

        # Modify TTL
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"createdAt": 1}, "expireAfterSeconds": 120})
        assert res.get("ok") == 1.0

        # Hide /unhide index by keyPattern / name
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"c": 1, "d": 1}, "hidden": True})
        assert res.get("ok") == 1.0
        res = src[db_name].command("collMod", coll_name, index={"name": "ab_index", "hidden": True})
        assert res.get("ok") == 1.0
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"c": 1, "d": 1}, "hidden": False})
        assert res.get("ok") == 1.0

        # PrepareUnique and dry run for unique
        res = src[db_name].command("collMod", coll_name, index={"name": "cd_index", "prepareUnique": True})
        assert res.get("ok") == 1.0
        res = src[db_name].command("collMod", coll_name, index={"name": "cd_index", "unique": True}, dryRun=True)
        assert res.get("ok") == 1.0

        # Prepare unique and convert to unique
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"c": 1}, "prepareUnique": True})
        assert res.get("ok") == 1.0
        res = src[db_name].command("collMod", coll_name, index={"keyPattern": {"c": 1}, "unique": True})
        assert res.get("ok") == 1.0

    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    assert csync.wait_for_repl_stage(), "Failed to finish init sync"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    time.sleep(1)
    index_docs = list(dst[db_name][coll_name].list_indexes())
    index_by_name = {idx["name"]: idx for idx in index_docs}
    assert index_by_name["createdAt_1"].get("expireAfterSeconds") == 120
    assert index_by_name["c_1"].get("unique") is True
    assert index_by_name["c_1"].get("hidden") is None
    assert index_by_name["ab_index"].get("hidden") is True
    assert index_by_name["cd_index"].get("prepareUnique") is True

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T18(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test collmod when converting to unique fails
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)

        db_name = "test_db"
        coll_name = "test_collection"
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        if src_cluster.is_sharded:
            src.admin.command({"enableSharding": db_name})
            src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"x": 1})
        src[db_name][coll_name].insert_many([
            {"x": 1},
            {"x": 1},
            {"x": 2}
        ])
        src[db_name][coll_name].create_index("x")

        assert csync.start(), "Failed to start csync service"

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

    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        if "operation_threads_1" in locals():
            for thread in operation_threads_1:
                thread.join()

    assert csync.wait_for_repl_stage(), "Failed to finish init sync"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    time.sleep(1)
    index_docs = list(dst[db_name][coll_name].list_indexes())
    idx = next(i for i in index_docs if i["name"] == "x_1")
    assert idx.get("unique") is not True, "Unique index should not be applied on destination"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "5"})
@pytest.mark.csync_log_level("trace")
@pytest.mark.parametrize(
    "clone_stage_pattern",
    [
        r'Starting "test_db\.collection_4" collection clone',
        r'Collection "test_db\.collection_4" created',
        r'read batch.*ns=test_db\.collection_4.*s=copy'
    ],
    ids=[
        "rename_at_clone_start",
        "rename_after_collection_created",
        "rename_during_batch_copy"
    ])
def test_csync_PML_T19(start_cluster, src_cluster, dst_cluster, csync, clone_stage_pattern):
    """
    Test to check renameCollection while collection is being cloned
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        db_name = "test_db"
        old_name = "collection_4"
        new_name = "renamed_collection_4"
        generate_dummy_data(src_cluster.connection, db_name, is_sharded=src_cluster.is_sharded)
        def start_csync():
            assert csync.start(), "Failed to start csync service"
        def rename_collection():
            log_stream = csync.logs(stream=True)
            pattern = re.compile(clone_stage_pattern)
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                if pattern.search(line):
                    break
            initial_docs_1 = [{"_id": i, "value": f"doc{i}", "email": f"user{i}@test.com", "field": i} for i in range(10)]
            initial_docs_2 = [{"_id": i + 10, "value": f"doc{i}", "email_new": f"user{i}@test.com", "field": i} for i in range(10)]
            src[db_name][old_name].insert_many(initial_docs_1)
            res = src.admin.command("renameCollection", f"{db_name}.{old_name}", to=f"{db_name}.{new_name}")
            assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
            src[db_name][new_name].insert_many(initial_docs_2)
        t1 = threading.Thread(target=start_csync)
        t2 = threading.Thread(target=rename_collection)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except Exception:
        raise
    assert csync.wait_for_repl_stage() is True, "Failed to finish init sync"
    assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
    assert csync.finalize() is True, "Failed to finalize csync service"
    time.sleep(1)
    dst_collections = dst[db_name].list_collection_names()
    assert new_name in dst_collections, "Renamed collection not found on destination"
    assert old_name not in dst_collections, "Old collection name still exists on destination"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "5"})
def test_csync_PML_T20(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check renameCollection during data clone.
    To use renameCollection in sharded cluster, source and destination collections
    must be on the same shard (if unsharded) and within the same database (if sharded)
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        generate_dummy_data(src_cluster.connection, "dummy", is_sharded=src_cluster.is_sharded)
        db_name1, db_name2 = "test_db1", "test_db2"
        old_name1, old_name2, old_name3  = "test_collection1", "test_collection2", "test_collection3"
        sh_old_name1, sh_old_name2  = "sh_test_collection1", "sh_test_collection2"
        new_name1, new_name2, new_name3 = "renamed_collection1", "renamed_collection2", "renamed_collection3"
        sh_new_name1, sh_new_name2 = "sh_renamed_collection1", "sh_renamed_collection2"
        if src_cluster.is_sharded:
            src.admin.command({"enableSharding": db_name1, "primaryShard": "rs1"})
            src.admin.command({"enableSharding": db_name2, "primaryShard": "rs1"})
            for coll_name in [sh_old_name1, sh_old_name2, sh_new_name2]:
                src.admin.command("shardCollection", f"{db_name1}.{coll_name}", key={"_id": "hashed"})
        initial_docs_1 = [{"_id": i, "value": f"doc{i}", "email": f"user{i}@test.com", "field": i} for i in range(10)]
        initial_docs_2 = [{"_id": i, "value": f"doc{i}", "email_new": f"user{i}@test.com", "field": i} for i in range(10)]
        for coll_name in [old_name1, old_name2, old_name3, sh_old_name1, sh_old_name2]:
            src[db_name1][coll_name].insert_many(initial_docs_1)
        src[db_name1][new_name2].insert_many(initial_docs_2)
        src[db_name1][sh_new_name2].insert_many(initial_docs_2)
        def start_csync():
            assert csync.start(), "Failed to start csync service"
        def rename_collection():
            log_stream = csync.logs(stream=True)
            watched_collections = {
                f"{db_name1}.{old_name1}": False,
                f"{db_name1}.{old_name2}": False,
                f"{db_name1}.{new_name2}": False,
                f"{db_name1}.{old_name3}": False,
            }
            if src_cluster.is_sharded:
                watched_collections.update({
                    f"{db_name1}.{sh_old_name1}": False,
                    f"{db_name1}.{sh_old_name2}": False,
                    f"{db_name1}.{sh_new_name2}": False})
            rename1_done = rename2_done = rename3_done = rename4_done = rename5_done = False
            for raw_line in log_stream:
                line = raw_line.decode("utf-8").strip()
                for coll in watched_collections:
                    if not watched_collections[coll] and f'Collection "{coll}" cloned' in line:
                        watched_collections[coll] = True
                if watched_collections[f"{db_name1}.{old_name1}"] and not rename1_done:
                    res = src.admin.command("renameCollection", f"{db_name1}.{old_name1}", to=f"{db_name1}.{new_name1}")
                    assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
                    rename1_done = True
                if (watched_collections[f"{db_name1}.{old_name2}"] and watched_collections[f"{db_name1}.{new_name2}"] and
                    not rename2_done):
                    res = src.admin.command("renameCollection", f"{db_name1}.{old_name2}", to=f"{db_name1}.{new_name2}", dropTarget=True)
                    assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
                    rename2_done = True
                if watched_collections[f"{db_name1}.{old_name3}"] and not rename3_done:
                    res = src.admin.command("renameCollection", f"{db_name1}.{old_name3}", to=f"{db_name2}.{new_name3}")
                    assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
                    rename3_done = True
                if src_cluster.is_sharded:
                    if watched_collections[f"{db_name1}.{sh_old_name1}"] and not rename4_done:
                        res = src.admin.command("renameCollection", f"{db_name1}.{sh_old_name1}", to=f"{db_name1}.{sh_new_name1}")
                        assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
                        rename4_done = True
                    if watched_collections[f"{db_name1}.{sh_old_name2}"] and watched_collections[f"{db_name1}.{sh_new_name2}"] and not rename5_done:
                        res = src.admin.command("renameCollection", f"{db_name1}.{sh_old_name2}", to=f"{db_name1}.{sh_new_name2}", dropTarget=True)
                        assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
                        rename5_done = True
                if src_cluster.is_sharded:
                    if rename1_done and rename2_done and rename3_done and rename4_done and rename5_done:
                        break
                else:
                    if rename1_done and rename2_done and rename3_done:
                        break
        t1 = threading.Thread(target=start_csync)
        t2 = threading.Thread(target=rename_collection)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except Exception:
        raise
    assert csync.wait_for_repl_stage() is True, "Failed to finish init sync"
    assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
    assert csync.finalize() is True, "Failed to finalize csync service"
    time.sleep(1)
    dst_collections1 = set(dst[db_name1].list_collection_names())
    dst_collections2 = set(dst[db_name2].list_collection_names())
    for name in [new_name1, new_name2]:
        assert name in dst_collections1, f"Expected collection '{name}' not found in {db_name1}"
    for name in [old_name1, old_name2, old_name3]:
        assert name not in dst_collections1, f"Old collection '{name}' still exists in {db_name1}"
    assert new_name3 in dst_collections2, f"Renamed collection '{new_name3}' not found in {db_name2}"
    if src_cluster.is_sharded:
        for name in [sh_new_name1, sh_new_name2]:
            assert name in dst_collections1, f"Expected collection '{name}' not found in {db_name1}"
        for name in [sh_old_name1, sh_old_name2]:
            assert name not in dst_collections1, f"Old collection '{name}' still exists in {db_name1}"
    result, summary = compare_data(src_cluster, dst_cluster)
    if not result:
        expected_mismatches = ["hash mismatch"]
        unexpected_mismatches = [mismatch for mismatch in summary if mismatch[1] not in expected_mismatches]
        if unexpected_mismatches:
            pytest.fail("Unexpected mismatches:\n" + "\n".join(str(m) for m in unexpected_mismatches))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T21(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check renameCollection during repl stage
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        db_name1, db_name2 = "test_db1", "test_db2"
        old_name1, old_name2, old_name3  = "test_collection1", "test_collection2", "test_collection3"
        sh_old_name1, sh_old_name2  = "sh_test_collection1", "sh_test_collection2"
        new_name1, new_name2, new_name3 = "renamed_collection1", "renamed_collection2", "renamed_collection3"
        sh_new_name1, sh_new_name2 = "sh_renamed_collection1", "sh_renamed_collection2"
        if src_cluster.is_sharded:
            src.admin.command({"enableSharding": db_name1, "primaryShard": "rs1"})
            src.admin.command({"enableSharding": db_name2, "primaryShard": "rs1"})
            for coll_name in [sh_old_name1, sh_old_name2, sh_new_name2]:
                src.admin.command("shardCollection", f"{db_name1}.{coll_name}", key={"_id": "hashed"})
        initial_docs = [{"_id": i, "value": f"doc{i}", "email": f"user{i}@test.com", "field": i} for i in range(10)]
        collection_names = [old_name1, old_name2, old_name3, new_name2]
        for coll_name in collection_names:
            src[db_name1][coll_name].insert_many(initial_docs)
        if src_cluster.is_sharded:
            for coll_name in [sh_old_name1, sh_old_name2]:
                src[db_name1][coll_name].insert_many(initial_docs)
            src[db_name1][sh_new_name2].insert_many(initial_docs)
        src[db_name1][new_name2].create_index("email", unique=True, name="email_unique22")
        src[db_name1][old_name3].create_index("email", unique=True, name="email_unique31")
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to finish init sync"
        repl_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        res = src.admin.command("renameCollection", f"{db_name1}.{old_name1}", to=f"{db_name1}.{new_name1}")
        assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
        res = src.admin.command("renameCollection", f"{db_name1}.{old_name2}", to=f"{db_name1}.{new_name2}", dropTarget=True)
        assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
        res = src.admin.command("renameCollection", f"{db_name1}.{old_name3}", to=f"{db_name2}.{new_name3}")
        assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
        if src_cluster.is_sharded:
            res = src.admin.command("renameCollection", f"{db_name1}.{sh_old_name1}", to=f"{db_name1}.{sh_new_name1}")
            assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
            res = src.admin.command("renameCollection", f"{db_name1}.{sh_old_name2}", to=f"{db_name1}.{sh_new_name2}", dropTarget=True)
            assert res.get("ok") == 1.0, f"renameCollection failed: {res}"
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
    time.sleep(1)
    dst_collections1 = set(dst[db_name1].list_collection_names())
    dst_collections2 = set(dst[db_name2].list_collection_names())
    for name in [new_name1, new_name2]:
        assert name in dst_collections1, f"Expected collection '{name}' not found in {db_name1}"
    for name in [old_name1, old_name2, old_name3]:
        assert name not in dst_collections1, f"Old collection '{name}' still exists in {db_name1}"
    assert new_name3 in dst_collections2, f"Renamed collection '{new_name3}' not found in {db_name2}"
    if src_cluster.is_sharded:
        for name in [sh_new_name1, sh_new_name2]:
            assert name in dst_collections1, f"Expected collection '{name}' not found in {db_name1}"
        for name in [sh_old_name1, sh_old_name2]:
            assert name not in dst_collections1, f"Old collection '{name}' still exists in {db_name1}"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"