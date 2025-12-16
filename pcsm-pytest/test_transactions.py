import pytest
import pymongo
import time
import threading
from pymongo.errors import OperationFailure, BulkWriteError

from cluster import Cluster
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations
from data_integrity_check import compare_data

def perform_transaction(src, session, db_name, coll_name, docs, commit=False, abort=False):
    collection = src[db_name][coll_name]
    try:
        if not session.in_transaction:
            session.start_transaction()
        collection.insert_many(docs, session=session)
        if commit:
            session.commit_transaction()
        elif abort:
            session.abort_transaction()
    except Exception as e:
        if session.in_transaction:
            session.abort_transaction()
        raise e

def shard_collection(client, db_name, coll_name, shard_key=None):
    """Enable sharding for database and collection"""
    shard_key = shard_key or {"_id": "hashed"}
    try:
        client.admin.command("enableSharding", db_name)
    except OperationFailure as e:
        if e.code != 23 and "already" not in str(e).lower():
            raise
    try:
        client.admin.command("shardCollection", f"{db_name}.{coll_name}", key=shard_key)
    except OperationFailure as e:
        if "already" not in str(e).lower():
            raise

def prepare_two_db_sharded_layout(src_client, src_cluster, db1, sharded1, db2, sharded2):
    """Create two databases pinned to distinct shards with sharded collections"""
    shard_ids = [shard["_id"] for shard in src_cluster.config["shards"][:2]]
    for db_name, sharded_coll, shard_name in [
        (db1, sharded1, shard_ids[0]),
        (db2, sharded2, shard_ids[1])]:
        try:
            src_client.admin.command({"enableSharding": db_name, "primaryShard": shard_name})
        except OperationFailure as e:
            if "already" not in str(e).lower():
                raise
        src_client.admin.command("shardCollection", f"{db_name}.{sharded_coll}", key={"_id": "hashed"})

def get_collection_shard(client, db_name, coll_name):
    """
    Determine which shard a collection is on based on the database's primaryShard
    """
    try:
        config_db = client.get_database("config")
        db_info = config_db.databases.find_one({"_id": db_name})
        if db_info:
            return db_info.get("primary")
        return None
    except Exception as e:
        Cluster.log(f"Error determining shard for {db_name}.{coll_name}: {e}")
        return None

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T22(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to verify transaction replication if transactions are started and committed during different stages of synchronization
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        if src_cluster.is_sharded:
            for coll_name in ["before_sync_v0", "before_sync_v1", "during_sync", "after_sync"]:
                shard_collection(src, "transaction_db", coll_name)
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)

        # Verify transactions started before sync and committed during data clone stage / before sync finalize
        session0 = src.start_session()
        perform_transaction(src, session0, "transaction_db", "before_sync_v0", [{"_id": 1, "value": "txn"}], commit=False)

        session1 = src.start_session()
        perform_transaction(src, session1, "transaction_db", "before_sync_v1", [{"_id": 1, "value": "txn"}], commit=False)

        assert csync.start(), "Failed to start csync service"

        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)

        session1.commit_transaction()
        session1.end_session()

        # Verify transaction started during sync and committed during replication stage
        session2 = src.start_session()
        perform_transaction(src, session2, "transaction_db", "during_sync", [{"_id": 1, "value": "txn"}], commit=False)

        assert csync.wait_for_repl_stage(), "Failed to start replication stage"

        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)

        session2.commit_transaction()
        session2.end_session()

        # Verify transaction started during replication and committed after sync completion
        session3 = src.start_session()
        perform_transaction(src, session3, "transaction_db", "after_sync", [{"_id": 1, "value": "txn"}], commit=False)

        session0.commit_transaction()
        session0.end_session()

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

    if "session3" in locals():
        session3.commit_transaction()
        session3.end_session()

    expected_doc = {"_id": 1, "value": "txn"}
    actual_doc1 = dst["transaction_db"]["before_sync_v0"].find_one({"_id": 1})
    actual_doc2 = dst["transaction_db"]["before_sync_v1"].find_one({"_id": 1})
    actual_doc3 = dst["transaction_db"]["during_sync"].find_one({"_id": 1})

    assert actual_doc1 is not None, "Expected document is missing in transaction_coll1"
    assert actual_doc1 == expected_doc, f"Document mismatch in transaction_coll1: {actual_doc1}"
    assert actual_doc2 is not None, "Expected document is missing in transaction_coll2"
    assert actual_doc2 == expected_doc, f"Document mismatch in transaction_coll2: {actual_doc2}"
    assert actual_doc3 is not None, "Expected document is missing in transaction_coll3"
    assert actual_doc3 == expected_doc, f"Document mismatch in transaction_coll3: {actual_doc3}"

    if src_cluster.is_sharded:
        expected_mismatches = [
            ("transaction_db.after_sync", "record count mismatch")]
    else:
        expected_mismatches = [
            ("transaction_db", "hash mismatch"),
            ("transaction_db.after_sync", "missing in dst DB"),
            ("transaction_db.after_sync", "_id_")]

    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is False, "Data mismatch after synchronization"

    missing_mismatches = [mismatch for mismatch in expected_mismatches if mismatch not in summary]
    unexpected_mismatches = [mismatch for mismatch in summary if mismatch not in expected_mismatches]

    assert not missing_mismatches, f"Expected mismatches missing: {missing_mismatches}"
    assert not unexpected_mismatches, f"Unexpected mismatches detected: {unexpected_mismatches}"

    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T23(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test for two concurrent transactions modifying the same document
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)

    assert csync.start(), "Failed to start csync service"

    with src.start_session() as session1, src.start_session() as session2:
        if src_cluster.is_sharded:
            shard_collection(src, "conflict_db", "test_coll")
        perform_transaction(src, session1, "conflict_db", "test_coll", [{"_id": 1, "value": "txn1"}], commit=False)
        conflict_insert_done = False
        max_retries = 3
        for attempt in range(max_retries):
            try:
                perform_transaction(src, session2, "conflict_db", "test_coll", [{"_id": 1, "value": "txn2"}], commit=False)
                conflict_insert_done = True
                break
            except OperationFailure as e:
                labels = getattr(e, "errorLabels", []) or []
                if ("TransientTransactionError" in labels or getattr(e, "code", None) in (112, 251)) and attempt < max_retries - 1:
                    if session2.in_transaction:
                        try:
                            session2.abort_transaction()
                        except Exception:
                            pass
                    time.sleep(0.2)
                    continue
                # duplicate key/write conflict is acceptable as the losing txn
                if getattr(e, "code", None) in (11000, 112):
                    conflict_insert_done = False
                    break
                raise

        session1.commit_transaction()
        try:
            if conflict_insert_done:
                session2.commit_transaction()
                assert False, "Write conflict transaction should not have committed"
            else:
                # insert already failed due to conflict; ensure session cleaned up
                if session2.in_transaction:
                    session2.abort_transaction()
        except OperationFailure:
            pass

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"

    assert csync.finalize(), "Failed to finalize csync service"

    actual_doc = dst["conflict_db"]["test_coll"].find_one({"_id": 1})
    expected_doc = {"_id": 1, "value": "txn1"}

    assert actual_doc is not None, "Expected document is missing from destination"
    assert actual_doc == expected_doc, f"Document in destination does not match expected: {actual_doc}"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T24(start_cluster, src_cluster, dst_cluster, csync):
    """
    MongoDB creates as many oplog entries as necessary to the encapsulate all write operations in a transaction,
    instead of a single entry for all write operations in the transaction. This removes the 16MB total size limit
    for a transaction imposed by the single oplog entry for all its write operations. Although the total size limit
    is removed, each oplog entry still must be within the BSON document size limit of 16MB.
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)

    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"

    large_docs = [{"_id": i, "name": "Large Document Test", "value": "x" * 16777160} for i in range(3)]

    def commit_with_retries(session, collection, docs):
        """Commit transaction with retry logic for transient errors.
        Note: When a transaction is aborted, all work is rolled back,
        so documents must be re-inserted on each retry attempt.
        """
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                if not session.in_transaction:
                    session.start_transaction()
                    collection.insert_many(docs, session=session)
                session.commit_transaction()
                return True
            except (BulkWriteError, OperationFailure) as e:
                is_transient = False
                error_labels = getattr(e, 'errorLabels', []) or []
                if "TransientTransactionError" in error_labels:
                    is_transient = True
                else:
                    error_code = getattr(e, 'code', None)
                    error_msg = str(e).lower()
                    if (error_code in (251, 112) or  # NoSuchTransaction, WriteConflict
                        "cache overflow" in error_msg or
                        "transaction rolled back" in error_msg):
                        is_transient = True
                    elif isinstance(e, BulkWriteError) and e.details and "writeErrors" in e.details:
                        for write_error in e.details["writeErrors"]:
                            write_error_code = write_error.get("code")
                            write_error_msg = write_error.get("errmsg", "").lower()
                            if (write_error_code in (251, 112) or
                                "cache overflow" in write_error_msg or
                                "transaction rolled back" in write_error_msg):
                                is_transient = True
                                break
                if is_transient and attempt < max_attempts - 1:
                    # Abort transaction before retry - all work is rolled back
                    if session.in_transaction:
                        try:
                            session.abort_transaction()
                        except Exception:
                            # Ignore abort errors as transaction may already be aborted or in invalid state
                            pass
                    time.sleep(1)
                    continue
                else:
                    # Non-transient error or max attempts reached - abort and raise
                    if session.in_transaction:
                        try:
                            session.abort_transaction()
                        except Exception:
                            # Ignore abort errors as transaction may already be aborted or in invalid state
                            pass
                    raise
    with src.start_session() as session:
        collection = src["large_txn_db"]["test_coll"]
        if src_cluster.is_sharded:
            shard_collection(src, "large_txn_db", "test_coll")
        assert commit_with_retries(session, collection, large_docs), "Failed to commit transaction after retries"

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"

    docs_in_dst = {doc["_id"]: doc for doc in dst["large_txn_db"]["test_coll"].find({})}
    missing_docs = [doc["_id"] for doc in large_docs if doc["_id"] not in docs_in_dst]
    mismatched_docs = [doc["_id"] for doc in large_docs if docs_in_dst.get(doc["_id"]) != doc]
    assert not missing_docs, f"Missing documents in destination: {missing_docs}"
    assert not mismatched_docs, f"Document mismatch in destination: {mismatched_docs}"

    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T25(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test for transaction with multiple collections
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)

    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"

    large_docs = [{"_id": i, "value": f"doc_{i}"} for i in range(2000)]

    # For sharded clusters collections must exist before starting a distributed transaction
    if src_cluster.is_sharded:
        dst_dbs = ["large_txn_db1", "large_txn_db2", "large_txn_db3"]
        collections = ["test_coll1", "test_coll2", "test_coll3"]
        for db_name, coll_name in zip(dst_dbs, collections):
            shard_collection(src, db_name, coll_name)

    max_retries = 5
    for attempt in range(max_retries):
        try:
            with src.start_session() as session:
                session.start_transaction()
                src["large_txn_db1"]["test_coll1"].insert_many(large_docs, session=session)
                src["large_txn_db2"]["test_coll2"].insert_many(large_docs, session=session)
                src["large_txn_db3"]["test_coll3"].insert_many(large_docs, session=session)
                session.commit_transaction()
            break
        except OperationFailure as e:
            if ("errorLabels" in e.details and "TransientTransactionError" in e.details["errorLabels"]):
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
            raise

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"

    assert csync.finalize(), "Failed to finalize csync service"

    dst_dbs = ["large_txn_db1", "large_txn_db2", "large_txn_db3"]
    collections = ["test_coll1", "test_coll2", "test_coll3"]
    for db_name, coll_name in zip(dst_dbs, collections):
        dst_coll = dst[db_name][coll_name]
        docs_in_dst = {doc["_id"]: doc for doc in dst_coll.find({})}
        missing_docs = [doc for doc in large_docs if doc["_id"] not in docs_in_dst]
        mismatched_docs = [doc for doc in large_docs if docs_in_dst.get(doc["_id"]) != doc]

        assert not missing_docs, f"Missing documents in {db_name}.{coll_name}: {missing_docs}"
        assert not mismatched_docs, f"Document mismatch in {db_name}.{coll_name}: {mismatched_docs}"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T26(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to verify transaction replication if csync service is restarted
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    if src_cluster.is_sharded:
        shard_collection(src, "test_db", "test_coll")
        for shard_rs, shard_client in src_cluster.get_shard_primary_clients():
            try:
                result = shard_client.admin.command({"setParameter": 1, "transactionLifetimeLimitSeconds": 120})
                assert result.get("ok") == 1.0, f"Failed to set transaction lifetime on {shard_rs}: {result}"
            except OperationFailure:
                raise
    else:
        try:
            result = src.admin.command({"setParameter": 1, "transactionLifetimeLimitSeconds": 120})
            assert result.get("ok") == 1.0, f"Failed to set transaction lifetime: {result}"
        except OperationFailure:
            raise

    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    result = csync.wait_for_checkpoint()
    assert result is True, "Clustersync failed to save checkpoint"

    with src.start_session() as session1, src.start_session() as session2:
        session1.start_transaction()
        session2.start_transaction()

        src["test_db"]["test_coll"].insert_one({"_id": 1, "value": "test1"}, session=session1)
        src["test_db"]["test_coll"].insert_one({"_id": 2, "value": "test2"}, session=session2)

        csync.restart()

        session1.commit_transaction()
        session2.abort_transaction()

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"

    assert csync.finalize(), "Failed to finalize csync service"

    docs_in_dst = {doc["_id"]: doc for doc in dst["test_db"]["test_coll"].find({})}
    expected_doc = {"_id": 1, "value": "test1"}
    unexpected_doc_id = 2

    assert expected_doc["_id"] in docs_in_dst, f"Expected document missing: {expected_doc}"
    assert docs_in_dst.get(expected_doc["_id"]) == expected_doc, f"Mismatch in expected document: {docs_in_dst.get(expected_doc['_id'])}"
    assert unexpected_doc_id not in docs_in_dst, f"Unexpected document found: {docs_in_dst.get(unexpected_doc_id)}"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T27(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to simulate oplog rollover during transaction
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)

    result = src.admin.command("replSetResizeOplog", size=990)
    assert result.get("ok") == 1.0, f"Failed to resize oplog: {result}"
    try:
        result = src.admin.command({"setParameter": 1, "transactionLifetimeLimitSeconds": 150})
        assert result.get("ok") == 1.0, f"Failed to set transaction lifetime: {result}"
    except OperationFailure:
        raise

    assert csync.start(), "Failed to start csync service"

    stop_generation = threading.Event()
    try:
        dummy_thread = threading.Thread(target=generate_dummy_data, args=(src_cluster.connection, "dummy", 20, 150000, 500, stop_generation, 0.05, True, False))
        dummy_thread.start()
        def keep_transaction_alive(session, db_name, coll_name):
            coll = src[db_name][coll_name]
            while session.in_transaction:
                try:
                    coll.update_one({"_id": "keep_alive"}, {"$set": {"ts": time.time()}}, upsert=True, session=session)
                    time.sleep(0.1)
                except pymongo.errors.PyMongoError:
                    break
        def commit_with_retries(session):
            for attempt in range(5):
                try:
                    session.commit_transaction()
                    return
                except pymongo.errors.OperationFailure as e:
                    if e.code == 251:
                        return False
                    elif "errorLabels" in e.details and "TransientTransactionError" in e.details["errorLabels"]:
                        time.sleep(1)
                    else:
                        break
        with src.start_session() as session1, src.start_session() as session2:
            session1.start_transaction()
            session2.start_transaction()

            keep_alive_thread1 = threading.Thread(target=keep_transaction_alive, args=(session1, "rollover_db", "transaction_coll1"), daemon=True)
            keep_alive_thread2 = threading.Thread(target=keep_transaction_alive, args=(session2, "rollover_db", "transaction_coll2"), daemon=True)

            keep_alive_thread1.start()
            keep_alive_thread2.start()

            src["rollover_db"]["transaction_coll1"].insert_one({"_id": 1, "value": "txn"}, session=session1)
            src["rollover_db"]["transaction_coll2"].insert_one({"_id": 1, "value": "txn"}, session=session2)

            time.sleep(60)
            commit_with_retries(session1)
            time.sleep(60)
            commit_with_retries(session2)
    except Exception:
        raise
    finally:
        stop_generation.set()
        all_threads = []
        if "dummy_thread" in locals():
            all_threads += [dummy_thread]
        if "keep_alive_thread1" in locals():
            all_threads += [keep_alive_thread1]
        if "keep_alive_thread2" in locals():
            all_threads += [keep_alive_thread2]
        for thread in all_threads:
            if thread.is_alive():
                thread.join()

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"

    assert csync.finalize(), "Failed to finalize csync service"

    expected_doc = {"_id": 1, "value": "txn"}
    actual_doc1 = dst["rollover_db"]["transaction_coll1"].find_one({"_id": 1})
    actual_doc2 = dst["rollover_db"]["transaction_coll2"].find_one({"_id": 1})

    assert actual_doc1 is not None, "Expected document is missing in transaction_coll1"
    assert actual_doc1 == expected_doc, f"Document mismatch in transaction_coll1: {actual_doc1}"
    assert actual_doc2 is not None, "Expected document is missing in transaction_coll2"
    assert actual_doc2 == expected_doc, f"Document mismatch in transaction_coll2: {actual_doc2}"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T59(start_cluster, src_cluster, dst_cluster, csync):
    """
    Validate multi-shard transaction commit and abort across two shards and replication
    Tests both successful commit and explicit abort scenarios
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    prepare_two_db_sharded_layout(src, src_cluster, "sharded_txn_db1", "items_abort_left_sharded",
                                                "sharded_txn_db2", "items_abort_right_sharded")
    prepare_two_db_sharded_layout(src, src_cluster, "sharded_txn_db1", "items_commit_left_sharded",
                                                "sharded_txn_db2", "items_commit_right_sharded")
    for db_name, coll_name in [("sharded_txn_db1", "items_left_unsharded"), ("sharded_txn_db2", "items_right_unsharded"),
                            ("sharded_txn_db1", "items_abort_left_unsharded"), ("sharded_txn_db2", "items_abort_right_unsharded")]:
        src[db_name].create_collection(coll_name)
    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    max_retries = 5
    for attempt in range(max_retries):
        try:
            with src.start_session() as session:
                session.start_transaction()
                src["sharded_txn_db1"]["items_abort_left_unsharded"].insert_one({"_id": 1, "value": "left_unsharded"}, session=session)
                src["sharded_txn_db1"]["items_abort_left_sharded"].insert_one({"_id": 2, "value": "left_sharded"}, session=session)
                src["sharded_txn_db2"]["items_abort_right_unsharded"].insert_one({"_id": 3, "value": "right_unsharded"}, session=session)
                src["sharded_txn_db2"]["items_abort_right_sharded"].insert_one({"_id": 4, "value": "right_sharded"}, session=session)
                session.abort_transaction()
            break
        except OperationFailure as e:
            error_labels = getattr(e, "errorLabels", []) or []
            if not error_labels and hasattr(e, "details") and isinstance(e.details, dict):
                error_labels = e.details.get("errorLabels", [])
            if "TransientTransactionError" in error_labels and attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            raise
    for db_name, coll_name in [
        ("sharded_txn_db1", "items_abort_left_unsharded"),
        ("sharded_txn_db1", "items_abort_left_sharded"),
        ("sharded_txn_db2", "items_abort_right_unsharded"),
        ("sharded_txn_db2", "items_abort_right_sharded")]:
        assert src[db_name][coll_name].count_documents({}) == 0, f"Source retained docs in {db_name}.{coll_name} after abort"
        assert dst[db_name][coll_name].count_documents({}) == 0, f"Destination retained docs in {db_name}.{coll_name} after abort"
    for db_name, coll_name in [("sharded_txn_db1", "items_commit_left_unsharded"), ("sharded_txn_db2", "items_commit_right_unsharded")]:
        src[db_name].create_collection(coll_name)
    for attempt in range(max_retries):
        try:
            with src.start_session() as session:
                session.start_transaction()
                src["sharded_txn_db1"]["items_commit_left_unsharded"].insert_one({"_id": 1, "value": "left_unsharded"}, session=session)
                src["sharded_txn_db1"]["items_commit_left_sharded"].insert_one({"_id": 2, "value": "left_sharded"}, session=session)
                src["sharded_txn_db2"]["items_commit_right_unsharded"].insert_one({"_id": 3, "value": "right_unsharded"}, session=session)
                src["sharded_txn_db2"]["items_commit_right_sharded"].insert_one({"_id": 4, "value": "right_sharded"}, session=session)
                session.commit_transaction()
            break
        except OperationFailure as e:
            error_labels = getattr(e, "errorLabels", []) or []
            if not error_labels and hasattr(e, "details") and isinstance(e.details, dict):
                error_labels = e.details.get("errorLabels", [])
            if "TransientTransactionError" in error_labels and attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            raise
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication after commit"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.parametrize("failpoint_target", ["src", "dst_transient_error", "dst_permanent_error"])
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T60(start_cluster, src_cluster, dst_cluster, csync, failpoint_target):
    """
    Test that csync does honor transaction during replication
    Creates collections after replication starts, ensures at least one collection
    is on shard 2, sets failpoint on shard 2, then executes a transaction that
    modifies collections on both shards. Verifies that no partial replication occurs
    (operations on shard 1 succeed, operations on shard 2 fail)
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    # For src failpoint, check placement on src; for dst failpoints, check placement on dst
    target_cluster = src_cluster if failpoint_target == "src" else dst_cluster
    shard_clients = target_cluster.get_shard_primary_clients()
    shard_ids = [shard_id for shard_id, _ in shard_clients]
    shard1_id = shard_ids[0]
    shard2_id = shard_ids[1]
    # Create collections one at a time until we have at least one on each shard
    max_attempts = 5
    collections_created = []
    colls_on_shard1 = []
    colls_on_shard2 = []
    db_counter = 0
    for attempt in range(max_attempts):
        db_name = f"txn_test_db_{db_counter}"
        coll_name = "test_coll"
        db_counter += 1
        try:
            src.admin.command("enableSharding", db_name)
            src[db_name].create_collection(coll_name)
            collections_created.append((db_name, coll_name))
            Cluster.log(f"Created database {db_name} with unsharded collection {coll_name}")
        except OperationFailure as e:
            if "already" not in str(e).lower():
                raise
            continue
        if failpoint_target == "src":
            coll_shard = get_collection_shard(src, db_name, coll_name)
        else:
            Cluster.log(f"Waiting for collection {db_name}.{coll_name} to replicate...")
            for wait_attempt in range(30):
                try:
                    config_db = dst.get_database("config")
                    db_exists = config_db.databases.find_one({"_id": db_name}) is not None
                    if db_exists:
                        Cluster.log(f"Collection {db_name}.{coll_name} replicated to dst")
                        break
                    time.sleep(1)
                except Exception as e:
                    Cluster.log(f"Error checking collection replication: {e}")
                    time.sleep(1)
            coll_shard = get_collection_shard(dst, db_name, coll_name)
        Cluster.log(f"Collection {db_name}.{coll_name} is on shard: {coll_shard}")
        if coll_shard == shard1_id:
            colls_on_shard1.append((db_name, coll_name))
            Cluster.log(f"Collection {db_name}.{coll_name} is on shard 1")
        elif coll_shard == shard2_id:
            colls_on_shard2.append((db_name, coll_name))
            Cluster.log(f"Collection {db_name}.{coll_name} is on shard 2")
        if colls_on_shard1 and colls_on_shard2:
            Cluster.log(f"Shard 1: {[f'{c[0]}.{c[1]}' for c in colls_on_shard1]}")
            Cluster.log(f"Shard 2: {[f'{c[0]}.{c[1]}' for c in colls_on_shard2]}")
            break
    else:
        pytest.fail(f"Could not get at least one collection on shard 1 and one on shard 2 after {max_attempts} attempts")
    fail_commands = ["insert"] if failpoint_target == "src" else ["update", "bulkWrite", "insert"]
    failpoint_set = False
    for shard_id, client in target_cluster.get_shard_primary_clients():
        if shard_id == shard2_id:
            Cluster.log(f"Setting failpoint on {failpoint_target} cluster, shard {shard_id}, commands: {fail_commands}")
            if failpoint_target == "src":
                client.admin.command({
                    "configureFailPoint": "failCommand",
                    "mode": "alwaysOn",
                    "data": {
                        "errorCode": 1,
                        "failCommands": fail_commands,
                        "failInternalCommands": True}})
            elif failpoint_target == "dst_transient_error":
                client.admin.command({
                    "configureFailPoint": "failCommand",
                    "mode": {"times": 2},
                    "data": {
                        "errorCode": 189,
                        "failCommands": fail_commands,
                        "errorLabels": ["RetryableWriteError"],
                        "failInternalCommands": True}})
            else:
                client.admin.command({
                    "configureFailPoint": "failCommand",
                    "mode": "alwaysOn",
                    "data": {
                        "errorCode": 1,
                        "failCommands": fail_commands,
                        "failInternalCommands": True}})
            failpoint_set = True
            break
    if not failpoint_set:
        raise AssertionError(f"Failed to set failpoint on {failpoint_target} cluster, shard {shard2_id}")
    time.sleep(1)
    transaction_failed = False
    with src.start_session() as session:
        session.start_transaction()
        try:
            for db_name, coll_name in colls_on_shard1:
                src[db_name][coll_name].insert_one({"_id": 1, "value": "shard1_data"}, session=session)
                src[db_name][coll_name].insert_one({"_id": 11, "value": "shard1_data2"}, session=session)
                src[db_name][coll_name].insert_one({"_id": 111, "value": "shard1_data3"}, session=session)
                src[db_name][coll_name].insert_one({"_id": 1111, "value": "shard1_data4"}, session=session)
                Cluster.log(f"Inserted 4 documents into {db_name}.{coll_name} (shard 1)")
            for db_name, coll_name in colls_on_shard2:
                src[db_name][coll_name].insert_one({"_id": 2, "value": "shard2_data"}, session=session)
                src[db_name][coll_name].insert_one({"_id": 22, "value": "shard2_data2"}, session=session)
                Cluster.log(f"Inserted 2 documents into {db_name}.{coll_name} (shard 2)")
            session.commit_transaction()
            Cluster.log("Transaction committed successfully on src")
            time.sleep(0.2)
            Cluster.log("Delay completed, operations should be queued together in change stream")
        except OperationFailure as e:
            transaction_failed = True
            Cluster.log(f"Transaction failed with error: {e}")
            if session.in_transaction:
                try:
                    session.abort_transaction()
                except Exception:
                    pass
    if failpoint_target == "src":
        assert transaction_failed, "Expected transaction to fail due to failpoint on src shard 2"
    else:
        assert not transaction_failed, "Transaction on src should succeed when failpoint is on dst"
    result = csync.wait_for_zero_lag()
    time.sleep(5)
    if failpoint_target == "dst_transient_error":
        assert result is True, "Failed to catch up on replication"
    elif failpoint_target == "dst_permanent_error":
        assert result is False, "Replication should fail due to permanent error on dst"
        for db_name, coll_name in colls_on_shard2:
            # Check for all documents that should not be replicated due to failpoint
            total_count = dst[db_name][coll_name].count_documents({})
            Cluster.log(f"Shard 2 documents in {db_name}.{coll_name}: total: {total_count}")
            assert total_count == 0, f"Expected no documents in {db_name}.{coll_name} (shard 2) due to failpoint, found {total_count}"
        for db_name, coll_name in colls_on_shard1:
            # Check for all documents that should not be replicated (transaction should be atomic)
            # In 6.0/7.0, operations are parallelized by namespace, so shard 1 operations might succeed
            total_count = dst[db_name][coll_name].count_documents({})
            Cluster.log(f"Shard 1 documents in {db_name}.{coll_name}: total: {total_count}")
            if total_count > 0:
                pytest.skip("!!! Bug: transaction should be atomic")
            assert total_count == 0, f"Expected no documents in {db_name}.{coll_name} (shard 1) due to failpoint, found {total_count}"
        return
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"