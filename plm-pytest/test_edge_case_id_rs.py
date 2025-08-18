import pytest
import pymongo
import docker
import uuid
import datetime
import random
import time
import threading
from bson import ObjectId, Decimal128, Binary, DBRef, Timestamp
from bson.binary import UUID_SUBTYPE
from cluster import Cluster
from perconalink import Perconalink
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

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T32(reset_state, srcRS, dstRS, plink):
    """
    Test case with various edge cases IDs
    """
    src = pymongo.MongoClient(srcRS.connection)

    db = src["stress_test_db"]
    collections_meta = [
        {"name": "regular_coll", "options": {}, "capped": False, "collation": False},
        {"name": "capped_coll", "options": {"capped": True, "size": 2147483648}, "capped": True, "collation": False}]
    for meta in collections_meta:
        db.create_collection(meta["name"], **meta["options"])
        meta["collection"] = db[meta["name"]]
    weird_ids = [
        Decimal128("9999999999999999999999999999.9999"), "", " " * 100, True, False,
        "🤞", 0, -1, 2**63 - 1, float("inf"), float("-inf"), None, ObjectId(),
        ObjectId("000000000000000000000000"), Decimal128("0.00000000000000000000000001"),
        datetime.datetime(1970, 1, 1), datetime.datetime(9999, 12, 31), Timestamp(2**31 - 1, 1),
        Binary.from_uuid(uuid.uuid4(), UUID_SUBTYPE), Binary(b"\x00"), Binary(b"\x00" * 16),
        Timestamp(0, 1), {"nested": "doc"}, {"level": {"deep": {"id": 42}}},
        DBRef("other_collection", ObjectId()), "simple_string", "123456",
        "some-uuid-format-1234-abcd", "special_chars_!@#$%^&*()", "كلمبالعربية",
        {"_id": "embedded"}, b"bytes_id", Binary(b"\xff" * 512), float("1e+300"),
        Decimal128("-9999999999999999999999999999.9999"), float("1e-300"),
        str(uuid.uuid4()), DBRef("coll", 12345), "a" * 512, "\u202eRTL_Text"]

    batch_size = 5000
    num_batches = 80
    for meta in collections_meta:
        weird_ids_copy = weird_ids.copy()
        def get_id():
            if random.random() < 0.7 and weird_ids:
                while weird_ids_copy:
                    _id = weird_ids_copy.pop()
                    return _id
            return ObjectId()
        collection = meta["collection"]
        for batch_num in range(num_batches):
            docs = []
            for i in range(batch_size):
                _id = get_id()
                doc = {
                    "_id": _id,
                    "payload": "x" * 5000,
                    "timestamp": datetime.datetime.now(datetime.timezone.utc),
                    "rand": random.random()}
                docs.append(doc)
            try:
                collection.insert_many(docs, ordered=False, bypass_document_validation=True)
            except pymongo.errors.BulkWriteError as e:
                Cluster.log(f"Insert failed in {meta['name']}: {e.details}")

    def run_crud_ops(iterations=100):
        for _ in range(iterations):
            for meta in collections_meta:
                ops = []
                for _ in range(100):
                    doc = {
                        "_id": ObjectId(),
                        "payload": "bulk_insert",
                        "timestamp": datetime.datetime.now(datetime.timezone.utc),
                        "rand": random.random()}
                    ops.append(pymongo.InsertOne(doc))
                ops.append(pymongo.DeleteMany({"rand": {"$gt": 0.95}}))
                try:
                    meta["collection"].bulk_write(ops, ordered=False)
                except pymongo.errors.BulkWriteError as e:
                    Cluster.log(f"Bulk ops failed on {meta['name']}: {e.details}")

    def start_plink():
        time.sleep(2)
        assert plink.start(), "Failed to start plink service"

    t1 = threading.Thread(target=start_plink)
    t2 = threading.Thread(target=run_crud_ops)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert plink.wait_for_repl_stage(), "Failed to start replication stage"
    assert plink.wait_for_zero_lag(), "Failed to catch up on replication"
    assert plink.finalize(), "Failed to finalize plink service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors: {error_logs}"

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
@pytest.mark.parametrize("id_type", ["float", "decimal"])
def test_rs_plink_PML_T34(reset_state, srcRS, dstRS, plink, id_type):
    """
    Test case with NaN ID
    """
    src = pymongo.MongoClient(srcRS.connection)

    db = src["stress_test_db"]
    collections_meta = [
        {"name": "regular_coll_pre", "options": {}, "capped": False, "collation": False},
        {"name": "regular_coll_post", "options": {}, "capped": False, "collation": False},
        {"name": "capped_coll_pre", "options": {"capped": True, "size": 2147483648}, "capped": True, "collation": False},
        {"name": "capped_coll_post", "options": {"capped": True, "size": 2147483648}, "capped": True, "collation": False}]

    for meta in collections_meta:
        db.create_collection(meta["name"], **meta["options"])
        meta["collection"] = db[meta["name"]]

    def add_data(target_suffix):
        batch_size = 2000
        num_batches = 80

        for meta in collections_meta:
            if not meta["name"].endswith(target_suffix):
                continue
            collection = meta["collection"]
            nan_id = float("nan") if id_type == "float" else Decimal128("NaN")
            collection.insert_one({"_id": nan_id, "payload": "x" * 5000})

            for _ in range(num_batches):
                docs = []
                for _ in range(batch_size):
                    _id = (
                        random.uniform(1e5, 1e10)
                        if id_type == "float"
                        else Decimal128(str(random.uniform(1e5, 1e10))))
                    doc = {
                        "_id": _id,
                        "payload": "x" * 5000,
                        "timestamp": datetime.datetime.now(datetime.timezone.utc),
                        "rand": random.random()}
                    docs.append(doc)
                try:
                    collection.insert_many(docs, ordered=False, bypass_document_validation=True)
                except pymongo.errors.BulkWriteError as e:
                    Cluster.log(f"Insert failed in {meta['name']}: {e.details}")
    add_data(target_suffix="_pre")
    assert plink.start(), "Failed to start plink service"
    assert plink.wait_for_repl_stage(), "Failed to start replication stage"
    add_data(target_suffix="_post")
    assert plink.wait_for_zero_lag(), "Failed to catch up on replication"
    assert plink.finalize(), "Failed to finalize plink service"

    expected_mismatches = [
        ("stress_test_db", "hash mismatch"),
        ("stress_test_db.regular_coll_pre", "hash mismatch")]

    result, summary = compare_data_rs(srcRS, dstRS)
    if not result:
        unexpected = [m for m in summary if m not in expected_mismatches]
        missing_expected = [m for m in expected_mismatches if m not in summary]

        if unexpected:
            assert False, f"Unexpected mismatches found: {unexpected}"
        if missing_expected:
            assert False, f"Expected mismatches missing: {missing_expected}"

    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors: {error_logs}"