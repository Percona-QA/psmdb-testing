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
from mongolink import Mongolink
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
def test_rs_mlink_PML_T32(reset_state, srcRS, dstRS, mlink):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    db = src["stress_test_db"]
    collections_meta = [
        {"name": "regular_coll", "options": {}, "capped": False, "collation": False},
        {"name": "capped_coll", "options": {"capped": True, "size": 2147483647}, "capped": True, "collation": False}]
    for meta in collections_meta:
        db.create_collection(meta["name"], **meta["options"])
        meta["collection"] = db[meta["name"]]
    weird_ids = [
        Decimal128("9999999999999999999999999999.9999"), "", " " * 100, True, False, None,
        "🤞", 0, -1, 2**63 - 1, float("inf"), float("-inf"), float("nan"), ObjectId(),
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

    def start_mlink():
        time.sleep(2)
        assert mlink.start(), "Failed to start mlink service"

    t1 = threading.Thread(target=start_mlink)
    t2 = threading.Thread(target=run_crud_ops)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert mlink.wait_for_repl_stage(), "Failed to start replication stage"
    assert mlink.wait_for_zero_lag(), "Failed to catch up on replication"
    assert mlink.finalize(), "Failed to finalize mlink service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors: {error_logs}"