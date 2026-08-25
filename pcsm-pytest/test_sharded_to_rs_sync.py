import pymongo
import pytest
from data_integrity_check import compare_data


@pytest.mark.parametrize("cluster_configs", ["sharded_rs"], indirect=True)
@pytest.mark.timeout(3600, func_only=True)
def test_pcsm_clone_skips_shard_ops_PLM_T(start_cluster, src_cluster, dst_cluster, csync):
    """
    Verify sharded collection copies from a sharded cluster to a replicaset cluster
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "clone_shard_test_db"
    coll_name = "sharded_coll"
    src.admin.command("enableSharding", db_name)
    collection = src[db_name][coll_name]
    collection.create_index([("region", pymongo.ASCENDING)])
    src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"region": 1})
    collection.insert_many([{"_id": i, "region": f"r_{i % 3}"} for i in range(200)])

    plain_coll = src[db_name]["plain_coll"]
    plain_coll.insert_many([{"_id": i, "value": f"v_{i}"} for i in range(50)])

    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    _, summary = compare_data(src_cluster, dst_cluster)
    expected_mismatch = (f"{db_name}.{coll_name}", "sharding status mismatch")
    assert summary == [expected_mismatch], f"Unexpected mismatches after synchronization: {summary}"
    assert dst[db_name][coll_name].count_documents({}) == 200
    assert dst[db_name]["plain_coll"].count_documents({}) == 50

    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"csync reported errors in logs: {error_logs}"


@pytest.mark.parametrize("cluster_configs", ["sharded_rs"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_pcsm_skips_shard_ops_mid_replication_PLM_T(start_cluster, src_cluster, dst_cluster, csync):
    """
    Verify a collection that has been sharded mid-replication does copy to a replicaset cluster
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "repl_shard_test_db"
    coll_name = "test_coll"
    src.admin.command("enableSharding", db_name)
    collection = src[db_name][coll_name]
    collection.insert_many([{"_id": i, "region": f"r_{i % 3}"} for i in range(100)])

    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    assert collection.count_documents({}) == dst[db_name][coll_name].count_documents({}) == 100

    collection.create_index([("region", pymongo.ASCENDING)])
    src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"region": 1})
    collection.insert_many([{"_id": i, "region": f"r_{i % 3}"} for i in range(100, 200)])

    expected_log = f'Skipping shard collection for "{db_name}.{coll_name}": target is not a sharded cluster'
    assert csync.wait_for_log(expected_log, timeout=60), (
        f"Expected skip message not found in logs: {csync.logs(tail=None)}"
    )

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    _, summary = compare_data(src_cluster, dst_cluster)
    expected_mismatch = (f"{db_name}.{coll_name}", "sharding status mismatch")
    assert summary == [expected_mismatch], f"Unexpected mismatches after synchronization: {summary}"
    assert dst[db_name][coll_name].count_documents({}) == 200

    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"csync reported errors in logs: {error_logs}"
