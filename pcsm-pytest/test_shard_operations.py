import pytest
import pymongo
import threading
import time
from pymongo.errors import OperationFailure
from cluster import Cluster
from data_integrity_check import compare_data


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
        src.admin.command(
            "refineCollectionShardKey", f"{db_name}.{coll_name}", key={"region": 1, "status": 1}
        )
    except OperationFailure as e:
        if "refineCollectionShardKey" in str(e) or "not supported" in str(e).lower():
            pytest.skip(f"refineCollectionShardKey not supported: {e}")
        raise
    result = csync.wait_for_zero_lag()
    if result:
        assert csync.finalize(), "Failed to finalize csync service"
        result, summary = compare_data(src_cluster, dst_cluster)
        expected_mismatch = (f"{db_name}.{coll_name}", "shard key mismatch")
        assert expected_mismatch in summary, (
            f"Expected shard key mismatch for {db_name}.{coll_name} not found in summary: {summary}"
        )
        csync_error, error_logs = csync.check_csync_errors()
        assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
    else:
        for _ in range(30):
            status = csync.status()
            Cluster.log(status)
            if not status["data"]["ok"] and status["data"]["state"] == "failed":
                break
            time.sleep(1)
        assert not status["data"]["ok"]
        assert status["data"]["state"] != "running"
        assert (
            status["data"]["error"]
            == "change replication: refineCollectionShardKey operation is not supported"
        )


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
        if (
            "reshardCollection" in str(e)
            or "not supported" in str(e).lower()
            or "command not found" in str(e).lower()
        ):
            pytest.skip(f"reshardCollection not supported: {e}")
        raise
    result = csync.wait_for_zero_lag()
    if result:
        assert csync.finalize(), "Failed to finalize csync service"
        result, summary = compare_data(src_cluster, dst_cluster)
        expected_mismatch = (f"{db_name}.{coll_name}", "shard key mismatch")
        assert expected_mismatch in summary, (
            f"Expected shard key mismatch for {db_name}.{coll_name} not found in summary: {summary}"
        )
        csync_error, error_logs = csync.check_csync_errors()
        assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
    else:
        for _ in range(30):
            status = csync.status()
            Cluster.log(status)
            if not status["data"]["ok"] and status["data"]["state"] == "failed":
                break
            time.sleep(1)
        assert not status["data"]["ok"]
        assert status["data"]["state"] != "running"
        assert (
            status["data"]["error"]
            == "change replication: reshardCollection operation is not supported"
        )


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
        if (
            "unshardCollection" in str(e)
            or "not supported" in str(e).lower()
            or "command not found" in str(e).lower()
        ):
            pytest.skip(f"unshardCollection not supported: {e}")
        raise
    result = csync.wait_for_zero_lag()
    if result:
        assert csync.finalize(), "Failed to finalize csync service"
        result, summary = compare_data(src_cluster, dst_cluster)
        expected_mismatch = (f"{db_name}.{coll_name}", "shard key mismatch")
        assert expected_mismatch in summary, (
            f"Expected shard key mismatch for {db_name}.{coll_name} not found in summary: {summary}"
        )
        csync_error, error_logs = csync.check_csync_errors()
        assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
    else:
        for _ in range(30):
            status = csync.status()
            Cluster.log(status)
            if not status["data"]["ok"] and status["data"]["state"] == "failed":
                break
            time.sleep(1)
        assert not status["data"]["ok"]
        assert status["data"]["state"] != "running"
        assert (
            status["data"]["error"]
            == "change replication: reshardCollection operation is not supported"
        )


@pytest.mark.skip(reason="movePrimary handling reverted in PCSM-299, tracked separately")
@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_csync_PML_T69(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test movePrimary and moveChunk with multiple collection types and concurrent writes
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    config_db = src.get_database("config")
    shards = list(config_db.shards.find())
    shard_names = [shard["_id"] for shard in shards]
    # Setup for movePrimary: multiple collection types in one database
    db1_name = "move_primary_test_db"
    src.admin.command("enableSharding", db1_name)
    plain_coll = src[db1_name]["plain_coll"]
    plain_coll.insert_many([{"_id": i, "value": f"v_{i}"} for i in range(100)])
    src[db1_name].create_collection(
        "validated_coll", validator={"status": {"$in": ["A", "B", "C"]}}
    )
    validated_coll = src[db1_name]["validated_coll"]
    validated_coll.insert_many([{"_id": i, "status": ["A", "B", "C"][i % 3]} for i in range(50)])
    indexed_coll = src[db1_name]["indexed_coll"]
    indexed_coll.insert_many(
        [{"_id": i, "region": f"r_{i % 5}", "status": f"s_{i % 3}"} for i in range(100)]
    )
    indexed_coll.create_index(
        [("region", pymongo.ASCENDING), ("status", pymongo.ASCENDING)], name="region_1_status_1"
    )
    src.admin.command("shardCollection", f"{db1_name}.sharded_coll", key={"_id": "hashed"})
    sharded_coll = src[db1_name]["sharded_coll"]
    sharded_coll.insert_many([{"_id": i, "data": f"d_{i}"} for i in range(200)])
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
    stop_event = threading.Event()

    def bg_writes():
        writer_client = pymongo.MongoClient(src_cluster.connection)
        coll = writer_client[db1_name]["plain_coll"]
        i = 10000
        while not stop_event.is_set():
            try:
                coll.insert_one({"_id": i, "ts": time.time()})
                i += 1
            except Exception:
                pass
            time.sleep(0.05)
        writer_client.close()

    writer = threading.Thread(target=bg_writes, daemon=True)
    writer.start()
    try:
        src.admin.command("movePrimary", db1_name, to=target_shard)
    except OperationFailure as e:
        if (
            "movePrimary" in str(e)
            or "not supported" in str(e).lower()
            or "command not found" in str(e).lower()
        ):
            pytest.skip(f"movePrimary not supported: {e}")
        raise
    finally:
        stop_event.set()
        writer.join(timeout=10)
        assert not writer.is_alive(), "background writer thread did not terminate after movePrimary"
    # Test moveChunk
    chunks = list(config_db.chunks.find({"ns": f"{db2_name}.test_coll"}))
    if chunks:
        source_shard = chunks[0].get("shard")
        target_shard2 = next((s for s in shard_names if s != source_shard), None)
        if target_shard2:
            try:
                src.admin.command(
                    "moveChunk", f"{db2_name}.test_coll", find={"_id": 0}, to=target_shard2
                )
            except OperationFailure as e:
                if (
                    "moveChunk" in str(e)
                    or "not supported" in str(e).lower()
                    or "command not found" in str(e).lower()
                ):
                    pytest.skip(f"moveChunk not supported: {e}")
                raise
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    for coll_name in ["plain_coll", "validated_coll", "indexed_coll", "sharded_coll"]:
        src_count = src[db1_name][coll_name].count_documents({})
        dst_count = dst[db1_name][coll_name].count_documents({})
        assert src_count == dst_count, f"{coll_name}: src={src_count} != dst={dst_count}"
        assert src_count > 0, f"{coll_name}: expected docs, got 0"
    dst_opts = dst[db1_name]["validated_coll"].options()
    assert "validator" in dst_opts, "Validator lost after movePrimary replication"
    dst_indexes = dst[db1_name]["indexed_coll"].index_information()
    assert "region_1_status_1" in dst_indexes, "Compound index lost after movePrimary replication"

    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
