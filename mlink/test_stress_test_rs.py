import pytest
import pymongo
import docker
import pymongo
import uuid
import datetime
import time
from bson import ObjectId, Decimal128, Binary, DBRef, Timestamp
from bson.binary import UUID_SUBTYPE

from cluster import Cluster
from mongolink import Mongolink
from data_types.basic_collection_types import create_collection_types, perform_crud_ops_collection
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
    collections_meta = create_collection_types(db)
    for meta in collections_meta:
        coll = meta["collection"]
        weird_ids = [
            "",
            " " * 100,
            True,
            False,
            None,
            "🤞",
            0,
            -1,
            2**63 - 1,
            float('inf'),
            float('-inf'),
            float('nan'),
            ObjectId("000000000000000000000000"),
            ObjectId(),
            Decimal128("0.00000000000000000000000001"),
            Decimal128("9999999999999999999999999999.9999"),
            datetime.datetime(1970, 1, 1),
            datetime.datetime(9999, 12, 31),
            Binary.from_uuid(uuid.uuid4(), UUID_SUBTYPE),
            Binary(b"\x00"),
            Binary(b"\x00" * 16),
            Timestamp(0, 1),
            {"nested": "doc"},
            {"level": {"deep": {"id": 42}}},
            DBRef("other_collection", ObjectId()),
            "simple_string",
            "123456",
            "some-uuid-format-1234-abcd",
            "special_chars_!@#$%^&*()",
            "كلمبالعربية",
        ]
        for _id in weird_ids:
            try:
                coll.insert_one({"_id": _id, "note": "weird id test", "inserted_at": datetime.datetime.now(datetime.timezone.utc)})
                coll.update_one({"_id": _id}, {"$set": {"note": "updated weird id"}})
            except Exception as e:
                Cluster.log(f"Insert failed for _id={_id}: {e}")

    def run_crud_ops(iterations=100):
        for _ in range(iterations):
            for meta in collections_meta:
                perform_crud_ops_collection(meta["collection"],capped=meta["capped"],timeseries=meta["timeseries"])

    run_crud_ops()
    assert mlink.start(), "Failed to start mlink service"
    run_crud_ops()

    assert mlink.wait_for_repl_stage(), "Failed to start replication stage"
    assert mlink.wait_for_zero_lag(), "Failed to catch up on replication"
    assert mlink.finalize(), "Failed to finalize mlink service"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors: {error_logs}"