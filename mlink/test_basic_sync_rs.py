import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading

from datetime import datetime
from cluster import Cluster
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

@pytest.fixture(scope="function")
def start_cluster(srcRS, dstRS,request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        srcRS.create()
        dstRS.create()
        yield True

    finally:
        if request.config.getoption("--verbose"):
            logs = Cluster.get_mlink_logs()
            print(f"\n\nmlink Last 50 Logs for {request.node.name}:\n{logs}\n\n")
        srcRS.destroy()
        dstRS.destroy()
        Cluster.destroy_mlink()


def test_rs_mlink_basic(start_cluster, srcRS, dstRS):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    src["test_db1"]["test_coll11"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db1"]["test_coll12"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db2"]["test_coll21"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db2"]["test_coll22"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db1"]["test_coll11"].create_index(["key"], name="test_coll11_index_old")

    mlink_container = Cluster.create_mlink(srcRS.pml_connection, dstRS.pml_connection)
    assert mlink_container is not None, "Failed to create mlink service"

    result = Cluster.start_mlink_service(mlink_container)
    assert result is True, "Failed to start mlink service"
    result = Cluster.finalize_mlink_service(mlink_container)
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


def test_rs_mlink_diff_data_types(start_cluster, srcRS, dstRS):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    init_test_db = create_all_types_db(srcRS.connection,"init_test_db")
    collections = [c for c in init_test_db.list_collection_names() if not c.startswith("system.")]

    mlink_container = Cluster.create_mlink(srcRS.pml_connection, dstRS.pml_connection)
    assert mlink_container is not None, "Failed to create mlink service"

    result = Cluster.start_mlink_service(mlink_container)
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

    result = Cluster.finalize_mlink_service(mlink_container)
    assert result is True, "Failed to finalize mlink service"

    result = Cluster.compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
