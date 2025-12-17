import pytest
import pymongo
import time
from pymongo.errors import OperationFailure
from cluster import Cluster
from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(300, func_only=True)
def test_csync_PML_T61(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM reaction to refineCollectionShardKey operation
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "refine_test_db"
    coll_name = "test_coll"
    src.admin.command("enableSharding", db_name)
    collection = src[db_name][coll_name]
    docs = [{"_id": i, "region": f"r_{i % 3}", "status": f"s_{i % 2}"} for i in range(10)]
    collection.insert_many(docs)
    collection.create_index([("region", pymongo.ASCENDING)])
    src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"region": 1})
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    assert collection.count_documents({}) == dst[db_name][coll_name].count_documents({}) == 10
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
        assert status['data']['error'] == "change replication: unsupported operation"

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(600, func_only=True)
def test_csync_PML_T62(start_cluster, src_cluster, dst_cluster, csync):
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
        assert status['data']['error'] == "change replication: unsupported operation"

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(600, func_only=True)
def test_csync_PML_T63(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM reaction to unshardCollection operation
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "unshard_test_db"
    coll_name = "test_coll"
    src.admin.command("enableSharding", db_name)
    collection = src[db_name][coll_name]
    docs = [{"_id": i, "value": f"data_{i}"} for i in range(2)]
    collection.insert_many(docs)
    collection.create_index([("_id", pymongo.HASHED)])
    src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"_id": "hashed"})
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    assert collection.count_documents({}) == dst[db_name][coll_name].count_documents({}) == 2
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
        assert status['data']['error'] == "change replication: unsupported operation"

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(600, func_only=True)
def test_csync_PML_T64(start_cluster, src_cluster, dst_cluster, csync):
    """
    Simple test to verify movePrimary and moveChunk commands
    """
    src = pymongo.MongoClient(src_cluster.connection)
    config_db = src.get_database("config")
    shards = list(config_db.shards.find())
    shard_names = [shard["_id"] for shard in shards]
    # Setup for movePrimary: unsharded collection
    db1_name = "move_primary_test_db"
    src.admin.command("enableSharding", db1_name)
    coll1 = src[db1_name]["test_coll"]
    coll1.insert_many([{"_id": i} for i in range(10)])
    # Setup for moveChunk: sharded collection
    db2_name = "move_chunk_test_db"
    src.admin.command("enableSharding", db2_name)
    coll2 = src[db2_name]["test_coll"]
    coll2.insert_many([{"_id": i} for i in range(100)])
    src.admin.command("shardCollection", f"{db2_name}.test_coll", key={"_id": 1})
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    # Test movePrimary
    db_info = config_db.databases.find_one({"_id": db1_name})
    current_primary = db_info.get("primary")
    target_shard = next((s for s in shard_names if s != current_primary), None)
    try:
        src.admin.command("movePrimary", db1_name, to=target_shard)
    except OperationFailure as e:
        if "movePrimary" in str(e) or "not supported" in str(e).lower() or "command not found" in str(e).lower():
            pytest.skip(f"movePrimary not supported: {e}")
        raise
    # Test moveChunk
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
    result, _ = compare_data(src_cluster, dst_cluster)
    if not result:
        pytest.xfail("Known limitation: PCSM-249")