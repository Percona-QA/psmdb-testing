import pytest
import pymongo

from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300,func_only=True)
def test_data_integrity_check_PML_T1(start_cluster, src_cluster, dst_cluster):
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)

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

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data should match after initial setup"

    collections_new = [
        ("test_db1", "test_coll8"),
        ("test_db1", "test_coll9"),
        ("test_db1", "test_coll10"),
        ("test_db1", "test_coll11"),
        ("test_db1", "test_coll12"),
    ]

    # Test 1: Capped collection options mismatch
    src["test_db1"].create_collection("test_coll8", capped=True, size=1024 * 1024, max=1000)
    dst["test_db1"].create_collection("test_coll8", capped=True, size=1024 * 1024, max=10)

    # Test 2: Collation options mismatch
    src["test_db1"].create_collection("test_coll9", collation={"locale": "fr"})
    dst["test_db1"].create_collection("test_coll9", collation={"locale": "en"})

    # Test 3: Change stream pre and post images options mismatch
    src["test_db1"].create_collection("test_coll10", changeStreamPreAndPostImages={"enabled": True})
    dst["test_db1"].create_collection("test_coll10")

    # Test 4: Missing collection in destination database
    src["test_db1"]["test_coll11"].insert_many(data)

    # Test 5: Missing collection in source database
    dst["test_db1"]["test_coll12"].insert_many(data)

    # Test 6: Record count mismatch
    dst["test_db1"]["test_coll7"].delete_one({"key": 9})

    is_sharded = (hasattr(src_cluster, "layout") and src_cluster.layout == "sharded") or \
                 (hasattr(dst_cluster, "layout") and dst_cluster.layout == "sharded")

    expected_mismatches = [
        ("test_db1.test_coll7", "record count mismatch"),
        ("test_db1.test_coll8", "options mismatch"),
        ("test_db1.test_coll9", "options mismatch"),
        ("test_db1.test_coll10", "options mismatch"),
        ("test_db1.test_coll11", "missing in dst DB"),
        ("test_db1.test_coll12", "missing in src DB"),
    ]

    # Hash mismatch is only checked for replica sets, not sharded clusters
    if not is_sharded:
        expected_mismatches.insert(0, ("test_db1", "hash mismatch"))

    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is False, "Data should not match after data modifications"

    for collection in expected_mismatches:
        assert collection in summary, \
            f"Mismatch for collection {collection} isn't detected"

    # Fix mismatches
    for db_name, coll_name in collections_new:
        dst[db_name][coll_name].drop_indexes()
        dst[db_name].drop_collection(coll_name)

    dst["test_db1"].create_collection("test_coll8", capped=True, size=1024 * 1024, max=1000)
    dst["test_db1"].create_collection("test_coll9", collation={"locale": "fr"})
    dst["test_db1"].create_collection("test_coll10", changeStreamPreAndPostImages={"enabled": True})
    src["test_db1"]["test_coll7"].delete_one({"key": 9})
    src["test_db1"].drop_collection("test_coll11")

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data should match again after reverting modifications"

    # Test index options mismatch
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

    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is False, "Data should not match after index modifications"

    for index in expected_mismatches:
        assert index in summary, \
            f"Mismatch for index {index} isn't detected"

    # Fix mismatches
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

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data should match again after reverting modifications"

    is_sharded = (hasattr(src_cluster, "layout") and src_cluster.layout == "sharded")
    if is_sharded:
        try:
            src.admin.command("enableSharding", "test_db2")
            dst.admin.command("enableSharding", "test_db2")
        except pymongo.errors.OperationFailure:
            pass

        shard_test_data = [{"shard_key": i, "value": f"data_{i}"} for i in range(20)]
        src.admin.command("shardCollection", "test_db2.sharded_coll1", key={"shard_key": 1})
        dst.admin.command("shardCollection", "test_db2.sharded_coll1", key={"shard_key": 1})
        src["test_db2"]["sharded_coll1"].insert_many(shard_test_data)
        dst["test_db2"]["sharded_coll1"].insert_many(shard_test_data)
        result, _ = compare_data(src_cluster, dst_cluster)
        assert result is True, "Data should match when sharding configuration is identical"

        # Test 1: One collection sharded on source but not on destination
        src["test_db2"]["sharded_coll2"].insert_many(shard_test_data)
        dst["test_db2"]["sharded_coll2"].insert_many(shard_test_data)
        src["test_db2"]["sharded_coll2"].create_index([("shard_key", pymongo.ASCENDING)])
        dst["test_db2"]["sharded_coll2"].create_index([("shard_key", pymongo.ASCENDING)])
        src.admin.command("shardCollection", "test_db2.sharded_coll2", key={"shard_key": 1})

        # Test 2: Different shard keys
        src["test_db2"]["sharded_coll3"].insert_many([{"shard_key": i, "alt_key": i, "value": f"data_{i}"} for i in range(20)])
        dst["test_db2"]["sharded_coll3"].insert_many([{"shard_key": i, "alt_key": i, "value": f"data_{i}"} for i in range(20)])
        src["test_db2"]["sharded_coll3"].create_index([("shard_key", pymongo.ASCENDING)])
        dst["test_db2"]["sharded_coll3"].create_index([("alt_key", pymongo.ASCENDING)])
        src.admin.command("shardCollection", "test_db2.sharded_coll3", key={"shard_key": 1})
        dst.admin.command("shardCollection", "test_db2.sharded_coll3", key={"alt_key": 1})

        # Test 3: One collection not sharded on source but sharded on destination
        src["test_db2"]["sharded_coll4"].insert_many(shard_test_data)
        dst.admin.command("shardCollection", "test_db2.sharded_coll4", key={"shard_key": 1})
        dst["test_db2"]["sharded_coll4"].insert_many(shard_test_data)

        # Test 4: Same shard key but different unique flag
        src["test_db2"]["sharded_coll5"].insert_many(shard_test_data)
        dst["test_db2"]["sharded_coll5"].insert_many(shard_test_data)
        src["test_db2"]["sharded_coll5"].create_index([("shard_key", pymongo.ASCENDING)], unique=True)
        dst["test_db2"]["sharded_coll5"].create_index([("shard_key", pymongo.ASCENDING)])
        src.admin.command("shardCollection", "test_db2.sharded_coll5", key={"shard_key": 1}, unique=True)
        dst.admin.command("shardCollection", "test_db2.sharded_coll5", key={"shard_key": 1})

        expected_sharding_mismatches = [
            ("test_db2.sharded_coll2", "sharding status mismatch"),
            ("test_db2.sharded_coll3", "shard key mismatch"),
            ("test_db2.sharded_coll4", "sharding status mismatch"),
            ("test_db2.sharded_coll5", "shard key unique flag mismatch"),
        ]

        result, summary = compare_data(src_cluster, dst_cluster)
        assert result is False, "Data should not match after sharding configuration mismatches"
        for mismatch in expected_sharding_mismatches:
            assert mismatch in summary, \
                f"Sharding mismatch for {mismatch} isn't detected"

        # Fix mismatches
        dst.admin.command("shardCollection", "test_db2.sharded_coll2", key={"shard_key": 1})
        dst["test_db2"].drop_collection("sharded_coll3")
        dst["test_db2"]["sharded_coll3"].insert_many([{"shard_key": i, "alt_key": i, "value": f"data_{i}"} for i in range(20)])
        dst["test_db2"]["sharded_coll3"].create_index([("shard_key", pymongo.ASCENDING)])
        dst.admin.command("shardCollection", "test_db2.sharded_coll3", key={"shard_key": 1})
        src["test_db2"]["sharded_coll4"].create_index([("shard_key", pymongo.ASCENDING)])
        src.admin.command("shardCollection", "test_db2.sharded_coll4", key={"shard_key": 1})
        dst["test_db2"].drop_collection("sharded_coll5")
        dst["test_db2"]["sharded_coll5"].insert_many(shard_test_data)
        dst["test_db2"]["sharded_coll5"].create_index([("shard_key", pymongo.ASCENDING)], unique=True)
        dst.admin.command("shardCollection", "test_db2.sharded_coll5", key={"shard_key": 1}, unique=True)

        result, _ = compare_data(src_cluster, dst_cluster)
        assert result is True, "Data should match again after fixing sharding configuration"
