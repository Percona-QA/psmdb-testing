import pytest
import pymongo
import time
import docker

from cluster import Cluster
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

@pytest.fixture(scope="function")
def start_cluster(srcRS, dstRS, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        srcRS.create()
        dstRS.create()
        yield True

    finally:
        srcRS.destroy()
        dstRS.destroy()

def test_data_integrity_check_PML_T1(start_cluster, srcRS, dstRS):
    src = pymongo.MongoClient(srcRS.connection)
    dst = pymongo.MongoClient(dstRS.connection)

    collections = [
        ("test_db1", "test_coll1"),
        ("test_db1", "test_coll2"),
        ("test_db1", "test_coll3"),
        ("test_db1", "test_coll4"),
        ("test_db1", "test_coll5"),
        ("test_db1", "test_coll6"),
        ("test_db1", "test_coll7"),
    ]

    for db_name, coll_name in collections:
        data = [{"key": i, "data": i} for i in range(10)]
        src[db_name][coll_name].insert_many(data)
        dst[db_name][coll_name].insert_many(data)

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data should match after initial setup"

    collections_new = [
        ("test_db1", "test_coll8"),
        ("test_db1", "test_coll9"),
        ("test_db1", "test_coll10"),
        ("test_db1", "test_coll11"),
        ("test_db1", "test_coll12"),
    ]

    src["test_db1"].create_collection("test_coll8", capped=True, size=1024 * 1024, max=1000)
    dst["test_db1"].create_collection("test_coll8", capped=True, size=1024 * 1024, max=10)

    src["test_db1"].create_collection("test_coll9", collation={"locale": "fr"})
    dst["test_db1"].create_collection("test_coll9", collation={"locale": "en"})

    src["test_db1"].create_collection("test_coll10", changeStreamPreAndPostImages={"enabled": True})
    dst["test_db1"].create_collection("test_coll10")

    src["test_db1"]["test_coll11"].insert_many(data)
    dst["test_db1"]["test_coll12"].insert_many(data)
    dst["test_db1"]["test_coll7"].delete_one({"key": 9})

    expected_mismatches = [
        ("test_db1", "hash mismatch"),
        ("test_db1.test_coll7", "record count mismatch"),
        ("test_db1.test_coll8", "options mismatch"),
        ("test_db1.test_coll9", "options mismatch"),
        ("test_db1.test_coll10", "options mismatch"),
        ("test_db1.test_coll11", "missing in dst DB"),
        ("test_db1.test_coll12", "missing in src DB"),
    ]

    result, summary = compare_data_rs(srcRS, dstRS)
    assert result is False, "Data should not match after data modifications"

    for collection in expected_mismatches:
        assert collection in summary, \
            f"Mismatch for collection {collection} isn't detected"

    for db_name, coll_name in collections_new:
        dst[db_name][coll_name].drop_indexes()
        dst[db_name].drop_collection(coll_name)

    dst["test_db1"].create_collection("test_coll8", capped=True, size=1024 * 1024, max=1000)
    dst["test_db1"].create_collection("test_coll9", collation={"locale": "fr"})
    dst["test_db1"].create_collection("test_coll10", changeStreamPreAndPostImages={"enabled": True})
    src["test_db1"]["test_coll7"].delete_one({"key": 9})
    src["test_db1"].drop_collection("test_coll11")

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data should match again after reverting modifications"

    src["test_db1"]["test_coll1"].create_index([("key", pymongo.ASCENDING)], name="index_basic")
    dst["test_db1"]["test_coll1"].create_index([("key", pymongo.DESCENDING)], name="index_basic")

    src["test_db1"]["test_coll1"].create_index([("key", pymongo.ASCENDING)], name="index_unique", unique=True)
    dst["test_db1"]["test_coll1"].create_index([("key", pymongo.ASCENDING)], name="index_unique")

    src["test_db1"]["test_coll2"].create_index([("key", pymongo.ASCENDING), ("data", pymongo.DESCENDING)], name="index_compound")
    dst["test_db1"]["test_coll2"].create_index([("key", pymongo.ASCENDING)], name="index_compound")

    src["test_db1"]["test_coll3"].create_index([("$**", pymongo.TEXT)], name="index_wildcard")
    dst["test_db1"]["test_coll3"].create_index([("$**", pymongo.ASCENDING)], name="index_wildcard")

    src["test_db1"]["test_coll4"].create_index([("key", pymongo.ASCENDING)], name="index_partial", partialFilterExpression={"data": {"$gt": 5}})
    dst["test_db1"]["test_coll4"].create_index([("key", pymongo.ASCENDING)], name="index_partial")

    src["test_db1"]["test_coll4"].create_index([("data", pymongo.ASCENDING)], name="index_sparse", sparse=True)
    dst["test_db1"]["test_coll4"].create_index([("data", pymongo.ASCENDING)], name="index_sparse")

    src["test_db1"]["test_coll5"].create_index([("data", pymongo.TEXT)], name="index_text")
    dst["test_db1"]["test_coll5"].create_index([("key", pymongo.TEXT)], name="index_text")

    src["test_db1"]["test_coll6"].create_index([("location", pymongo.GEOSPHERE)], name="index_geo")
    dst["test_db1"]["test_coll6"].create_index([("location", pymongo.GEO2D)], name="index_geo")

    src["test_db1"]["test_coll7"].create_index([("createdAt", pymongo.ASCENDING)], name="index_ttl", expireAfterSeconds=3600)
    dst["test_db1"]["test_coll7"].create_index([("createdAt", pymongo.ASCENDING)], name="index_ttl", expireAfterSeconds=7200)

    expected_mismatches = [
        ("test_db1.test_coll1", "index_basic"),
        ("test_db1.test_coll1", "index_unique"),
        ("test_db1.test_coll2", "index_compound"),
        ("test_db1.test_coll3", "index_wildcard"),
        ("test_db1.test_coll4", "index_partial"),
        ("test_db1.test_coll4", "index_sparse"),
        ("test_db1.test_coll5", "index_text"),
        ("test_db1.test_coll6", "index_geo"),
        ("test_db1.test_coll7", "index_ttl")
    ]

    result, summary = compare_data_rs(srcRS, dstRS)
    assert result is False, "Data should not match after index modifications"

    for index in expected_mismatches:
        assert index in summary, \
            f"Mismatch for index {index} isn't detected"

    for db_name, coll_name in collections:
        dst[db_name][coll_name].drop_indexes()

    dst["test_db1"]["test_coll1"].create_index([("key", pymongo.ASCENDING)], name="index_basic")
    dst["test_db1"]["test_coll1"].create_index([("key", pymongo.ASCENDING)], name="index_unique", unique=True)
    dst["test_db1"]["test_coll2"].create_index([("key", pymongo.ASCENDING), ("data", pymongo.DESCENDING)], name="index_compound")
    dst["test_db1"]["test_coll3"].create_index([("$**", pymongo.TEXT)], name="index_wildcard")
    dst["test_db1"]["test_coll4"].create_index([("key", pymongo.ASCENDING)], name="index_partial", partialFilterExpression={"data": {"$gt": 5}})
    dst["test_db1"]["test_coll4"].create_index([("data", pymongo.ASCENDING)], name="index_sparse", sparse=True)
    dst["test_db1"]["test_coll5"].create_index([("data", pymongo.TEXT)], name="index_text")
    dst["test_db1"]["test_coll6"].create_index([("location", pymongo.GEOSPHERE)], name="index_geo")
    dst["test_db1"]["test_coll7"].create_index([("createdAt", pymongo.ASCENDING)], name="index_ttl", expireAfterSeconds=3600)

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data should match again after reverting modifications"
