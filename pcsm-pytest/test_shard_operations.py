import threading
import time

import pymongo
import pytest
from cluster import Cluster
from data_integrity_check import compare_data
from pymongo.errors import OperationFailure, PyMongoError


@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(300, func_only=True)
def test_csync_PML_T66(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM reaction to refineCollectionShardKey operation
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "refine_test_db"
    coll_name = "test_coll"
    src.admin.command("enableSharding", db_name)
    collection = src[db_name][coll_name]
    src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"region": 1})
    collection.insert_one({"_id": 0, "region": "r_0", "status": "s_0"})
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    assert collection.count_documents({}) == dst[db_name][coll_name].count_documents({}) == 1
    collection.create_index([("region", pymongo.ASCENDING), ("status", pymongo.ASCENDING)])
    try:
        src.admin.command("refineCollectionShardKey", f"{db_name}.{coll_name}",
                         key={"region": 1, "status": 1})
    except OperationFailure as e:
        if "refineCollectionShardKey" in str(e) or "not supported" in str(e).lower():
            pytest.skip(f"refineCollectionShardKey not supported: {e}")
        raise
    result = csync.wait_for_zero_lag()
    if result:
        assert csync.finalize(), "Failed to finalize csync service"
        result, summary = compare_data(src_cluster, dst_cluster)
        expected_mismatch = (f"{db_name}.{coll_name}", "shard key mismatch")
        assert expected_mismatch in summary, f"Expected shard key mismatch for {db_name}.{coll_name} not found in summary: {summary}"
        csync_error, error_logs = csync.check_csync_errors()
        assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
    else:
        for _ in range(30):
            status = csync.status()
            Cluster.log(status)
            if not status['data']['ok'] and status['data']['state'] == 'failed':
                break
            time.sleep(1)
        assert not status['data']['ok']
        assert status['data']['state'] != 'running'
        assert status['data']['error'] == "change replication: refineCollectionShardKey operation is not supported"

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(600, func_only=True)
def test_csync_PML_T67(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM reaction to reshardCollection operation
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "reshard_test_db"
    coll_name = "test_coll"
    src.admin.command("enableSharding", db_name)
    collection = src[db_name][coll_name]
    docs = [{"_id": i, "region": f"r_{i % 3}"} for i in range(1000)]
    collection.insert_many(docs)
    src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"_id": 1})
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    assert collection.count_documents({}) == dst[db_name][coll_name].count_documents({}) == 1000
    try:
        src.admin.command("reshardCollection", f"{db_name}.{coll_name}", key={"_id": "hashed"})
    except OperationFailure as e:
        if "reshardCollection" in str(e) or "not supported" in str(e).lower() or "command not found" in str(e).lower():
            pytest.skip(f"reshardCollection not supported: {e}")
        raise
    result = csync.wait_for_zero_lag()
    if result:
        assert csync.finalize(), "Failed to finalize csync service"
        result, summary = compare_data(src_cluster, dst_cluster)
        expected_mismatch = (f"{db_name}.{coll_name}", "shard key mismatch")
        assert expected_mismatch in summary, f"Expected shard key mismatch for {db_name}.{coll_name} not found in summary: {summary}"
        csync_error, error_logs = csync.check_csync_errors()
        assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
    else:
        for _ in range(30):
            status = csync.status()
            Cluster.log(status)
            if not status['data']['ok'] and status['data']['state'] == 'failed':
                break
            time.sleep(1)
        assert not status['data']['ok']
        assert status['data']['state'] != 'running'
        assert status['data']['error'] == "change replication: reshardCollection operation is not supported"

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(600, func_only=True)
def test_csync_PML_T68(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM reaction to unshardCollection operation
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "unshard_test_db"
    coll_name = "test_coll"
    src.admin.command("enableSharding", db_name)
    collection = src[db_name][coll_name]
    src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"_id": "hashed"})
    collection.insert_one({"_id": 0, "value": "data_0"})
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    assert collection.count_documents({}) == dst[db_name][coll_name].count_documents({}) == 1
    try:
        src.admin.command("unshardCollection", f"{db_name}.{coll_name}")
    except OperationFailure as e:
        if "unshardCollection" in str(e) or "not supported" in str(e).lower() or "command not found" in str(e).lower():
            pytest.skip(f"unshardCollection not supported: {e}")
        raise
    result = csync.wait_for_zero_lag()
    if result:
        assert csync.finalize(), "Failed to finalize csync service"
        result, summary = compare_data(src_cluster, dst_cluster)
        expected_mismatch = (f"{db_name}.{coll_name}", "shard key mismatch")
        assert expected_mismatch in summary, f"Expected shard key mismatch for {db_name}.{coll_name} not found in summary: {summary}"
        csync_error, error_logs = csync.check_csync_errors()
        assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
    else:
        for _ in range(30):
            status = csync.status()
            Cluster.log(status)
            if not status['data']['ok'] and status['data']['state'] == 'failed':
                break
            time.sleep(1)
        assert not status['data']['ok']
        assert status['data']['state'] != 'running'
        assert status['data']['error'] == "change replication: reshardCollection operation is not supported"

def _database_primary(client, db_name):
    entry = client.get_database("config").databases.find_one({"_id": db_name})
    assert entry is not None, f"database {db_name} missing from config.databases"
    return entry["primary"]

def _is_sharded_collection(client, ns):
    doc = client.get_database("config").collections.find_one({"_id": ns})
    if doc is None or doc.get("dropped") or doc.get("unsplittable"):
        return False
    return doc.get("key") is not None

def _collection_presence_by_shard(cluster, db_name, coll_name):
    """Return {shard_id: count} when the collection name exists, else {shard_id: None}."""
    presence = {}
    for shard_rs, shard_client in cluster.get_shard_primary_clients():
        try:
            names = shard_client[db_name].list_collection_names()
            if coll_name in names:
                presence[shard_rs] = shard_client[db_name][coll_name].count_documents({})
            else:
                presence[shard_rs] = None
        finally:
            shard_client.close()
    return presence

def _assert_unsharded_placement(cluster, db_name, coll_name, primary_shard, expected_count):
    """Unsharded collection must live on the primary and be absent from other shards.

    None means the collection name is absent; 0 means an empty leftover namespace.
    """
    presence = _collection_presence_by_shard(cluster, db_name, coll_name)
    Cluster.log(f"{db_name}.{coll_name} shard presence={presence} primary={primary_shard}")
    assert presence.get(primary_shard) == expected_count, (
        f"{db_name}.{coll_name}: expected {expected_count} docs on primary shard "
        f"{primary_shard}, got {presence}")
    leftovers = {shard: n for shard, n in presence.items()
                 if shard != primary_shard and n is not None}
    assert not leftovers, (
        f"{db_name}.{coll_name}: collection name still present on non-primary shards: "
        f"{leftovers}")

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_csync_PML_T69(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test movePrimary and moveChunk with multiple collection types and concurrent writes.
    After movePrimary, post-move insert/update/delete must replicate, unsharded
    collections must not leave empty namespaces on non-primary shards, and dest
    data/metadata must match.
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    config_db = src.get_database("config")
    shards = list(config_db.shards.find())
    shard_names = [shard["_id"] for shard in shards]
    db1_name = "move_primary_test_db"
    unsharded_colls = ["plain_coll", "validated_coll", "indexed_coll"]
    src.admin.command("enableSharding", db1_name)
    plain_coll = src[db1_name]["plain_coll"]
    plain_coll.insert_many([{"_id": i, "value": f"v_{i}"} for i in range(100)])
    src[db1_name].create_collection("validated_coll", validator={"status": {"$in": ["A", "B", "C"]}})
    validated_coll = src[db1_name]["validated_coll"]
    validated_coll.insert_many([{"_id": i, "status": ["A", "B", "C"][i % 3]} for i in range(50)])
    indexed_coll = src[db1_name]["indexed_coll"]
    indexed_coll.insert_many([{"_id": i, "region": f"r_{i % 5}", "status": f"s_{i % 3}"} for i in range(100)])
    indexed_coll.create_index([("region", pymongo.ASCENDING), ("status", pymongo.ASCENDING)], name="region_1_status_1")
    src.admin.command("shardCollection", f"{db1_name}.sharded_coll", key={"_id": "hashed"})
    sharded_coll = src[db1_name]["sharded_coll"]
    sharded_coll.insert_many([{"_id": i, "data": f"d_{i}"} for i in range(200)])
    db2_name = "move_chunk_test_db"
    src.admin.command("enableSharding", db2_name)
    coll2 = src[db2_name]["test_coll"]
    coll2.insert_many([{"_id": i} for i in range(100)])
    src.admin.command("shardCollection", f"{db2_name}.test_coll", key={"_id": 1})
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    original_primary = _database_primary(src, db1_name)
    target_shard = next((s for s in shard_names if s != original_primary), None)
    if target_shard is None:
        pytest.skip("movePrimary requires at least 2 source shards")
    stop_event = threading.Event()
    def bg_writes():
        writer_client = pymongo.MongoClient(src_cluster.connection)
        coll = writer_client[db1_name]["plain_coll"]
        i = 10000
        while not stop_event.is_set():
            try:
                coll.insert_one({"_id": i, "ts": time.time()})
                i += 1
            except PyMongoError as e:
                Cluster.log(f"background writer skipped insert _id={i}: {e}")
            time.sleep(0.05)
        writer_client.close()
    writer = threading.Thread(target=bg_writes, daemon=True)
    writer.start()
    try:
        src.admin.command("movePrimary", db1_name, to=target_shard)
    finally:
        stop_event.set()
        writer.join(timeout=10)
        assert not writer.is_alive(), "background writer thread did not terminate after movePrimary"
    src_primary_after = _database_primary(src, db1_name)
    assert src_primary_after == target_shard and src_primary_after != original_primary, (
        f"movePrimary did not move source primary: before={original_primary}, "
        f"after={src_primary_after}, expected={target_shard}")
    post_move_id = 99999
    plain_coll.insert_many([{"_id": i, "value": "post_move"} for i in range(100, 110)])
    plain_coll.update_many({"_id": {"$lt": 10}}, {"$set": {"value": "updated_post_move"}})
    plain_coll.delete_many({"_id": {"$gte": 105}})
    validated_coll.insert_one({"_id": post_move_id, "status": "A"})
    validated_coll.update_one({"_id": 0}, {"$set": {"status": "C"}})
    validated_coll.delete_one({"_id": 1})
    indexed_coll.insert_one({"_id": post_move_id, "region": "r_post", "status": "s_post"})
    indexed_coll.update_one({"_id": 0}, {"$set": {"region": "r_post"}})
    indexed_coll.delete_one({"_id": 1})
    sharded_coll.insert_one({"_id": post_move_id, "data": "post_move"})
    sharded_coll.update_one({"_id": 0}, {"$set": {"data": "updated_post_move"}})
    sharded_coll.delete_one({"_id": 1})
    chunks = list(config_db.chunks.find({"ns": f"{db2_name}.test_coll"}))
    if chunks:
        source_shard = chunks[0].get("shard")
        target_shard2 = next((s for s in shard_names if s != source_shard), None)
        if target_shard2:
            try:
                src.admin.command("moveChunk", f"{db2_name}.test_coll", find={"_id": 0}, to=target_shard2)
            except OperationFailure as e:
                if "moveChunk" in str(e) or "not supported" in str(e).lower() or "command not found" in str(e).lower():
                    pytest.skip(f"moveChunk not supported: {e}")
                raise
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    dst_primary = _database_primary(dst, db1_name)
    for coll_name in unsharded_colls:
        expected = src[db1_name][coll_name].count_documents({})
        _assert_unsharded_placement(
            src_cluster, db1_name, coll_name, src_primary_after, expected)
        _assert_unsharded_placement(
            dst_cluster, db1_name, coll_name, dst_primary, expected)
        assert not _is_sharded_collection(dst, f"{db1_name}.{coll_name}"), (
            f"{db1_name}.{coll_name} became sharded on destination after movePrimary")
    src_sharded_key = src["config"]["collections"].find_one({"_id": f"{db1_name}.sharded_coll"})
    dst_sharded_key = dst["config"]["collections"].find_one({"_id": f"{db1_name}.sharded_coll"})
    assert _is_sharded_collection(src, f"{db1_name}.sharded_coll")
    assert _is_sharded_collection(dst, f"{db1_name}.sharded_coll")
    assert src_sharded_key["key"] == dst_sharded_key["key"]
    assert dst[db1_name]["plain_coll"].find_one({"_id": 0})["value"] == "updated_post_move"
    assert dst[db1_name]["plain_coll"].find_one({"_id": 100})["value"] == "post_move"
    assert dst[db1_name]["plain_coll"].find_one({"_id": 109}) is None
    assert dst[db1_name]["validated_coll"].find_one({"_id": post_move_id})["status"] == "A"
    assert dst[db1_name]["validated_coll"].find_one({"_id": 0})["status"] == "C"
    assert dst[db1_name]["validated_coll"].find_one({"_id": 1}) is None
    assert dst[db1_name]["indexed_coll"].find_one({"_id": post_move_id}) is not None
    assert dst[db1_name]["indexed_coll"].find_one({"_id": 0})["region"] == "r_post"
    assert dst[db1_name]["indexed_coll"].find_one({"_id": 1}) is None
    assert dst[db1_name]["sharded_coll"].find_one({"_id": post_move_id})["data"] == "post_move"
    assert dst[db1_name]["sharded_coll"].find_one({"_id": 0})["data"] == "updated_post_move"
    assert dst[db1_name]["sharded_coll"].find_one({"_id": 1}) is None
    for coll_name in ["plain_coll", "validated_coll", "indexed_coll", "sharded_coll"]:
        src_count = src[db1_name][coll_name].count_documents({})
        dst_count = dst[db1_name][coll_name].count_documents({})
        assert src_count == dst_count, f"{coll_name}: src={src_count} != dst={dst_count}"
        assert src_count > 0, f"{coll_name}: expected docs, got 0"
    dst_opts = dst[db1_name]["validated_coll"].options()
    assert "validator" in dst_opts, "Validator lost after movePrimary replication"
    dst_indexes = dst[db1_name]["indexed_coll"].index_information()
    assert "region_1_status_1" in dst_indexes, "Compound index lost after movePrimary replication"
    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Data mismatch after movePrimary: {summary}"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"