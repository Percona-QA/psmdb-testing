import threading
import time

import pymongo
import pytest
from data_integrity_check import compare_data


@pytest.mark.parametrize("cluster_configs", ["sharded_rs"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_pcsm_clone_skips_shard_ops_PML_T109(start_cluster, src_cluster, dst_cluster, csync):
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
def test_pcsm_skips_shard_ops_mid_replication_PML_T110(start_cluster, src_cluster, dst_cluster, csync):
    """
    Verify a collection that has been sharded mid-replication copies to a replicaset cluster
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

@pytest.mark.parametrize("cluster_configs", ["sharded_rs"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_pcsm_skips_shard_ops_after_restart_PML_T111(start_cluster, src_cluster, dst_cluster, csync):
    """
    Verify PCSM still skips sharding operations against a replicaset target when
    restarted while a shardCollection event is in flight while writes still taking place
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "restart_shard_test_db"
    coll_name = "test_coll"
    src.admin.command("enableSharding", db_name)
    collection = src[db_name][coll_name]
    collection.insert_many([{"_id": i, "region": f"r_{i % 3}"} for i in range(100)])

    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    assert collection.count_documents({}) == dst[db_name][coll_name].count_documents({}) == 100

    stop_event = threading.Event()
    def bg_writes():
        writer_client = pymongo.MongoClient(src_cluster.connection)
        coll = writer_client[db_name][coll_name]
        i = 10000
        while not stop_event.is_set():
            try:
                coll.insert_one({"_id": i, "region": f"r_{i % 3}"})
                i += 1
            except Exception:
                pass
            time.sleep(0.05)
        writer_client.close()
    writer = threading.Thread(target=bg_writes, daemon=True)
    writer.start()

    assert csync.wait_for_checkpoint(), "Failed to save checkpoint"

    collection.create_index([("region", pymongo.ASCENDING)])
    src.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"region": 1})
    time.sleep(0.2)
    assert csync.restart(), "Failed to restart csync"

    stop_event.set()
    writer.join(timeout=10)
    assert not writer.is_alive(), "background writer thread did not terminate after restart"

    expected_log = f'Skipping shard collection for "{db_name}.{coll_name}": target is not a sharded cluster'
    assert csync.wait_for_log(expected_log, timeout=60), (
        f"Expected skip message not found in logs: {csync.logs(tail=None)}"
    )

    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"

    _, summary = compare_data(src_cluster, dst_cluster)
    expected_mismatch = (f"{db_name}.{coll_name}", "sharding status mismatch")
    assert summary == [expected_mismatch], f"Unexpected mismatches after synchronization: {summary}"
    src_count = collection.count_documents({})
    assert src_count > 100, "background writer did not insert any documents"
    assert dst[db_name][coll_name].count_documents({}) == src_count

    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "detected concurrent process"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))
