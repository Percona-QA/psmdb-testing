import pytest
import pymongo
import time
import docker
import threading
from pymongo.errors import OperationFailure

from cluster import Cluster
from perconalink import Perconalink
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
def plink(srcRS,dstRS):
    return Perconalink('plink',srcRS.plink_connection, dstRS.plink_connection)

@pytest.fixture(scope="module")
def start_cluster(srcRS, dstRS, plink, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        srcRS.create()
        dstRS.create()
        yield True

    finally:
        srcRS.destroy()
        dstRS.destroy()
        plink.destroy()

@pytest.fixture(scope="function")
def reset_state(srcRS, dstRS, plink, request):
    src_client = pymongo.MongoClient(srcRS.connection)
    dst_client = pymongo.MongoClient(dstRS.connection)
    def print_logs():
        if request.config.getoption("--verbose"):
            logs = plink.logs()
            print(f"\n\nplink Last 50 Logs for plink:\n{logs}\n\n")
    request.addfinalizer(print_logs)
    plink.destroy()
    for db_name in src_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            src_client.drop_database(db_name)
    for db_name in dst_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            dst_client.drop_database(db_name)
    plink.create()

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

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T22(reset_state, srcRS, dstRS, plink):
    """
    Test to verify transaction replication if transactions are started and committed during different stages of synchronization
    """
    try:
        src = pymongo.MongoClient(srcRS.connection)
        dst = pymongo.MongoClient(dstRS.connection)

        generate_dummy_data(srcRS.connection)
        init_test_db, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)

        # Verify transactions started before sync and committed during data clone stage / before sync finalize
        session0 = src.start_session()
        perform_transaction(src, session0, "transaction_db", "before_sync_v0", [{"_id": 1, "value": "txn"}], commit=False)

        session1 = src.start_session()
        perform_transaction(src, session1, "transaction_db", "before_sync_v1", [{"_id": 1, "value": "txn"}], commit=False)

        result = plink.start()
        assert result is True, "Failed to start plink service"

        clone_test_db, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)

        session1.commit_transaction()
        session1.end_session()

        # Verify transaction started during sync and committed during replication stage
        session2 = src.start_session()
        perform_transaction(src, session2, "transaction_db", "during_sync", [{"_id": 1, "value": "txn"}], commit=False)

        result = plink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"

        repl_test_db, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)

        session2.commit_transaction()
        session2.end_session()

        # Verify transaction started during replication and committed after sync completion
        session3 = src.start_session()
        perform_transaction(src, session3, "transaction_db2", "after_sync", [{"_id": 1, "value": "txn"}], commit=False)

        time.sleep(10)

        session0.commit_transaction()
        session0.end_session()

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

    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"

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
        ("transaction_db2", "missing in dst DB"),
        ("transaction_db2.after_sync", "missing in dst DB"),
        ("transaction_db2.after_sync", "_id_"),
    ]

    result, summary = compare_data_rs(srcRS, dstRS)
    assert result is False, "Data mismatch after synchronization"

    missing_mismatches = [mismatch for mismatch in expected_mismatches if mismatch not in summary]
    unexpected_mismatches = [mismatch for mismatch in summary if mismatch not in expected_mismatches]

    assert not missing_mismatches, f"Expected mismatches missing: {missing_mismatches}"
    assert not unexpected_mismatches, f"Unexpected mismatches detected: {unexpected_mismatches}"

    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T23(reset_state, srcRS, dstRS, plink):
    """
    Test for two concurrent transactions modifying the same document
    """
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    result = plink.start()
    assert result is True, "Failed to start plink service"

    with src.start_session() as session1, src.start_session() as session2:
        perform_transaction(src, session1, "conflict_db", "test_coll", [{"_id": 1, "value": "txn1"}], commit=False)
        perform_transaction(src, session2, "conflict_db", "test_coll", [{"_id": 1, "value": "txn2"}], commit=False)

        session1.commit_transaction()
        try:
            session2.commit_transaction()
            assert False, "Write conflict transaction should not have committed"
        except OperationFailure:
            pass

    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"

    actual_doc = dst["conflict_db"]["test_coll"].find_one({"_id": 1})
    expected_doc = {"_id": 1, "value": "txn1"}

    assert actual_doc is not None, "Expected document is missing from destination"
    assert actual_doc == expected_doc, f"Document in destination does not match expected: {actual_doc}"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T24(reset_state, srcRS, dstRS, plink):
    """
    MongoDB creates as many oplog entries as necessary to the encapsulate all write operations in a transaction,
    instead of a single entry for all write operations in the transaction. This removes the 16MB total size limit
    for a transaction imposed by the single oplog entry for all its write operations. Although the total size limit
    is removed, each oplog entry still must be within the BSON document size limit of 16MB.
    """
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    result = plink.start()
    assert result is True, "Failed to start plink service"
    result = plink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"

    large_docs = [{"_id": i, "name": "Large Document Test", "value": "x" * 16777160} for i in range(10)]

    with src.start_session() as session:
        session.start_transaction()
        src["large_txn_db"]["test_coll"].insert_many(large_docs, session=session)
        session.commit_transaction()

    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"

    docs_in_dst = {doc["_id"]: doc for doc in dst["large_txn_db"]["test_coll"].find({})}
    missing_docs = [doc["_id"] for doc in large_docs if doc["_id"] not in docs_in_dst]
    mismatched_docs = [doc["_id"] for doc in large_docs if docs_in_dst.get(doc["_id"]) != doc]

    assert not missing_docs, f"Missing documents in destination: {missing_docs}"
    assert not mismatched_docs, f"Document mismatch in destination: {mismatched_docs}"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T25(reset_state, srcRS, dstRS, plink):
    """
    Test for transaction with multiple collections
    """
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    result = plink.start()
    assert result is True, "Failed to start plink service"
    result = plink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"

    large_docs = [{"_id": i, "value": f"doc_{i}"} for i in range(2000)]

    with src.start_session() as session:
        session.start_transaction()
        src["large_txn_db1"]["test_coll1"].insert_many(large_docs, session=session)
        src["large_txn_db2"]["test_coll2"].insert_many(large_docs, session=session)
        src["large_txn_db3"]["test_coll3"].insert_many(large_docs, session=session)
        session.commit_transaction()

    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"

    dst_dbs = ["large_txn_db1", "large_txn_db2", "large_txn_db3"]
    collections = ["test_coll1", "test_coll2", "test_coll3"]
    for db_name, coll_name in zip(dst_dbs, collections):
        dst_coll = dst[db_name][coll_name]
        docs_in_dst = {doc["_id"]: doc for doc in dst_coll.find({})}
        missing_docs = [doc for doc in large_docs if doc["_id"] not in docs_in_dst]
        mismatched_docs = [doc for doc in large_docs if docs_in_dst.get(doc["_id"]) != doc]

        assert not missing_docs, f"Missing documents in {db_name}.{coll_name}: {missing_docs}"
        assert not mismatched_docs, f"Document mismatch in {db_name}.{coll_name}: {mismatched_docs}"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors in logs: {error_logs}"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T26(reset_state, srcRS, dstRS, plink):
    """
    Test to verify transaction replication if plink service is restarted
    """
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)
    result = src.admin.command("setParameter", 1, transactionLifetimeLimitSeconds=120)
    assert result.get("ok") == 1.0, f"Failed to set transaction lifetime: {result}"

    result = plink.start()
    assert result is True, "Failed to start plink service"
    result = plink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"
    result = plink.wait_for_checkpoint()
    assert result is True, "Perconalink failed to save checkpoint"

    with src.start_session() as session1, src.start_session() as session2:
        session1.start_transaction()
        session2.start_transaction()

        src["test_db"]["test_coll"].insert_one({"_id": 1, "value": "test1"}, session=session1)
        src["test_db"]["test_coll"].insert_one({"_id": 2, "value": "test2"}, session=session2)

        plink.restart()

        session1.commit_transaction()
        session2.abort_transaction()

    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"

    docs_in_dst = {doc["_id"]: doc for doc in dst["test_db"]["test_coll"].find({})}
    expected_doc = {"_id": 1, "value": "test1"}
    unexpected_doc_id = 2

    assert expected_doc["_id"] in docs_in_dst, f"Expected document missing: {expected_doc}"
    assert docs_in_dst.get(expected_doc["_id"]) == expected_doc, f"Mismatch in expected document: {docs_in_dst.get(expected_doc['_id'])}"
    assert unexpected_doc_id not in docs_in_dst, f"Unexpected document found: {docs_in_dst.get(unexpected_doc_id)}"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T27(reset_state, srcRS, dstRS, plink):
    """
    Test to simulate oplog rollover during transaction
    """
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    result = src.admin.command("replSetResizeOplog", size=990)
    assert result.get("ok") == 1.0, f"Failed to resize oplog: {result}"
    result = src.admin.command("setParameter", 1, transactionLifetimeLimitSeconds=150)
    assert result.get("ok") == 1.0, f"Failed to set transaction lifetime: {result}"

    result = plink.start()
    assert result is True, "Failed to start plink service"

    stop_generation = threading.Event()
    try:
        dummy_thread = threading.Thread(target=generate_dummy_data,args=(srcRS.connection, "dummy", 20, 150000, 500, stop_generation, 0.05))
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
    except Exception as e:
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

    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"

    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"

    expected_doc = {"_id": 1, "value": "txn"}
    actual_doc1 = dst["rollover_db"]["transaction_coll1"].find_one({"_id": 1})
    actual_doc2 = dst["rollover_db"]["transaction_coll2"].find_one({"_id": 1})

    assert actual_doc1 is not None, "Expected document is missing in transaction_coll1"
    assert actual_doc1 == expected_doc, f"Document mismatch in transaction_coll1: {actual_doc1}"
    assert actual_doc2 is not None, "Expected document is missing in transaction_coll2"
    assert actual_doc2 == expected_doc, f"Document mismatch in transaction_coll2: {actual_doc2}"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors in logs: {error_logs}"