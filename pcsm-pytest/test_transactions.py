import pytest
import pymongo
import time
import threading
from pymongo.errors import OperationFailure, BulkWriteError

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

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T22(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to verify transaction replication if transactions are started and committed during different stages of synchronization
    """
    try:
        src = pymongo.MongoClient(src_cluster.connection)
        dst = pymongo.MongoClient(dst_cluster.connection)
        is_sharded = src_cluster.layout == "sharded"
        init_test_db, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)

        # Verify transactions started before sync and committed during data clone stage / before sync finalize
        session0 = src.start_session()
        perform_transaction(src, session0, "transaction_db", "before_sync_v0", [{"_id": 1, "value": "txn"}], commit=False)

        session1 = src.start_session()
        perform_transaction(src, session1, "transaction_db", "before_sync_v1", [{"_id": 1, "value": "txn"}], commit=False)

        assert csync.start(), "Failed to start csync service"

        clone_test_db, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=is_sharded)

        session1.commit_transaction()
        session1.end_session()

        # Verify transaction started during sync and committed during replication stage
        session2 = src.start_session()
        perform_transaction(src, session2, "transaction_db", "during_sync", [{"_id": 1, "value": "txn"}], commit=False)

        assert csync.wait_for_repl_stage(), "Failed to start replication stage"

        repl_test_db, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)

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

    expected_mismatches = [
        ("transaction_db.after_sync", "missing in dst DB"),
        ("transaction_db.after_sync", "_id_")]
    if not is_sharded:
        expected_mismatches.append(("transaction_db", "hash mismatch"))

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
        perform_transaction(src, session1, "conflict_db", "test_coll", [{"_id": 1, "value": "txn1"}], commit=False)
        perform_transaction(src, session2, "conflict_db", "test_coll", [{"_id": 1, "value": "txn2"}], commit=False)

        session1.commit_transaction()
        try:
            session2.commit_transaction()
            assert False, "Write conflict transaction should not have committed"
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
                elif hasattr(e, 'code') and e.code in (251, 112):  # NoSuchTransaction, WriteConflict
                    is_transient = True
                elif isinstance(e, BulkWriteError) and e.details and "writeErrors" in e.details:
                    for write_error in e.details["writeErrors"]:
                        error_code = write_error.get("code")
                        error_msg = write_error.get("errmsg", "")
                        if error_code in (251, 112):
                            is_transient = True
                            break
                        if "cache overflow" in error_msg.lower() or "transaction rolled back" in error_msg.lower():
                            is_transient = True
                            break
                elif "cache overflow" in str(e).lower() or "transaction rolled back" in str(e).lower():
                    is_transient = True
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
        commit_with_retries(session, collection, large_docs)

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"

    assert csync.finalize(), "Failed to finalize csync service"

    docs_in_dst = {doc["_id"]: doc for doc in dst["large_txn_db"]["test_coll"].find({})}
    missing_docs = [doc["_id"] for doc in large_docs if doc["_id"] not in docs_in_dst]
    mismatched_docs = [doc["_id"] for doc in large_docs if docs_in_dst.get(doc["_id"]) != doc]

    assert not missing_docs, f"Missing documents in destination: {missing_docs}"
    assert not mismatched_docs, f"Document mismatch in destination: {mismatched_docs}"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
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
    is_sharded = src_cluster.layout == "sharded"

    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"

    large_docs = [{"_id": i, "value": f"doc_{i}"} for i in range(2000)]

    # For sharded clusters collections must exist before starting a distributed transaction
    if is_sharded:
        dst_dbs = ["large_txn_db1", "large_txn_db2", "large_txn_db3"]
        collections = ["test_coll1", "test_coll2", "test_coll3"]
        for db_name, coll_name in zip(dst_dbs, collections):
            try:
                src.admin.command("enableSharding", db_name)
            except pymongo.errors.OperationFailure:
                pass
            try:
                src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"_id": "hashed"})
            except pymongo.errors.OperationFailure:
                raise

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
    is_sharded = src_cluster.layout == "sharded"
    if is_sharded:
        for shard in src_cluster.config['shards']:
            shard_primary = shard['members'][0]['host']
            shard_rs = shard['_id']
            try:
                shard_client = pymongo.MongoClient(f"mongodb://root:root@{shard_primary}:27017/?replicaSet={shard_rs}")
                result = shard_client.admin.command({"setParameter": 1, "transactionLifetimeLimitSeconds": 120})
                assert result.get("ok") == 1.0, f"Failed to set transaction lifetime on {shard_primary}: {result}"
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

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T27(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to simulate oplog rollover during transaction
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    is_sharded = src_cluster.layout == "sharded"

    if is_sharded:
        for shard in src_cluster.config['shards']:
            shard_primary = shard['members'][0]['host']
            shard_rs = shard['_id']
            shard_client = pymongo.MongoClient(f"mongodb://root:root@{shard_primary}:27017/?replicaSet={shard_rs}")
            result = shard_client.admin.command("replSetResizeOplog", size=990)
            assert result.get("ok") == 1.0, f"Failed to resize oplog on {shard_primary}: {result}"
            try:
                result = shard_client.admin.command({"setParameter": 1, "transactionLifetimeLimitSeconds": 150})
                assert result.get("ok") == 1.0, f"Failed to set transaction lifetime on {shard_primary}: {result}"
            except OperationFailure:
                shard_client.close()
                raise
            shard_client.close()
    else:
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
        dummy_thread = threading.Thread(target=generate_dummy_data, args=(src_cluster.connection, "dummy", 20, 150000, 500, stop_generation, 0.05, True, is_sharded))
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