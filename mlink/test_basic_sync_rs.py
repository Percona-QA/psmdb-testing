import pytest
import pymongo
import bson
import testinfra
import threading
import time
import os
import docker
import threading
import debugpy

from datetime import datetime
from cluster import Cluster
from mongolink import Mongolink
from db_setup import create_all_types_db


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def dstRS():
    return Cluster({ "_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]})

@pytest.fixture(scope="package")
def srcRS():
    return Cluster({ "_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]})

@pytest.fixture(scope="package")
def mlink(srcRS,dstRS):
    return Mongolink('mlink',srcRS.pml_connection, dstRS.pml_connection)

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


def test_rs_mlink_basic(start_cluster, srcRS, dstRS, mlink):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    src["test_db1"]["test_coll11"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db1"]["test_coll12"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db2"]["test_coll21"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db2"]["test_coll22"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db1"]["test_coll11"].create_index(["key"], name="test_coll11_index_old")

    result = mlink.start()
    assert result is True, "Failed to start mlink service"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result = Cluster.compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"

    src["test_db2"]["test_coll21"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db3"]["test_coll31"].insert_many([{"key": i, "data": i} for i in range(10)])
    dst["test_db4"]["test_coll41"].insert_many([{"key": i, "data": i} for i in range(10)])

    src["test_db3"]["test_coll31"].create_index(["key"], name="test_coll31_index_old")
    dst["test_db1"]["test_coll11"].drop_index('test_coll11_index_old')
    dst["test_db1"]["test_coll11"].create_index(["data"], name="test_coll11_index_old")
    dst["test_db1"]["test_coll11"].create_index(["key"], name="test_coll11_index_new")

    result = Cluster.compare_data_rs(srcRS, dstRS)
    assert result is False, "Data should not match after modification in dst"


def test_rs_mlink_diff_data_types(start_cluster, srcRS, dstRS, mlink):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    init_test_db = create_all_types_db(srcRS.connection,"init_test_db")
    collections = [c for c in init_test_db.list_collection_names() if not c.startswith("system.")]

    result = mlink.start()
    assert result is True, "Failed to start mlink service"

    for coll in collections:
        collection = init_test_db[coll]
        if collection.count_documents({}) > 0:
            collection.update_one({}, {"$set": {"updated_field": "updated_value"}})
            collection.update_many({}, {"$set": {"status": "updated"}})
            collection.delete_one({})
            collection.delete_many({"status": "old_status"})

    repl_test_db = create_all_types_db(srcRS.connection,"repl_test_db")

    for coll in collections:
        collection = repl_test_db[coll]
        if collection.count_documents({}) > 0:
            collection.update_one({}, {"$set": {"updated_field": "updated_value"}})
            collection.update_many({}, {"$set": {"status": "updated"}})
            collection.delete_one({})
            collection.delete_many({"status": "old_status"})
    time.sleep(10)

    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result = Cluster.compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"


def test_rs_mlink_drop_db(start_cluster, srcRS, dstRS, mlink):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    src["test_db1"]["test_coll"].insert_many([{"key": i, "data": f"original_{i}"} for i in range(10)])
    src["test_db1"]["test_coll"].create_index([("key", pymongo.ASCENDING)], name="test_index")

    def start_mlink():
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        Cluster.log("mlink started successfully.")

    def drop_db():
        time.sleep(0.1)
        src.drop_database("test_db1")

    mlink_thread = threading.Thread(target=start_mlink)
    drop_db_thread = threading.Thread(target=drop_db)

    mlink_thread.start()
    drop_db_thread.start()

    mlink_thread.join()
    drop_db_thread.join()

    repl_test_db = create_all_types_db(srcRS.connection,"repl_test_db")
    src["test_db1"]["test_coll"].insert_many([{"key": i, "data": f"new_{i}"} for i in range(20)])
    src["test_db1"]["test_coll"].create_index([("data", pymongo.ASCENDING)], name="test_index_new")
    src.drop_database("repl_test_db")
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result = Cluster.compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after recreating dropped database"

def test_rs_mlink_drop_collection(start_cluster, srcRS, dstRS, mlink):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    src["test_db1"]["test_coll"].insert_many([{"key": i, "data": f"original_{i}"} for i in range(10)])
    src["test_db1"]["test_coll"].create_index([("key", pymongo.ASCENDING)], name="test_index")

    def start_mlink():
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        Cluster.log("mlink started successfully.")

    def drop_collection():
        time.sleep(0.1)
        src["test_db1"]["test_coll"].drop()

    mlink_thread = threading.Thread(target=start_mlink)
    drop_collection_thread = threading.Thread(target=drop_collection)

    mlink_thread.start()
    drop_collection_thread.start()

    mlink_thread.join()
    drop_collection_thread.join()

    src["test_db1"]["test_coll"].insert_many([{"key": i, "data": f"new_{i}"} for i in range(5)])
    src["test_db1"]["test_coll"].create_index([("data", pymongo.ASCENDING)], name="test_index_new")

    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"

    result = Cluster.compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after recreating dropped database"
