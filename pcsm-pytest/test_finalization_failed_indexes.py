from datetime import datetime

import pytest
import pymongo

from data_integrity_check import get_indexes

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.timeout(300, func_only=True)
def test_pcsm_status_finalization_section_PCSM_T95(start_cluster, src_cluster, dst_cluster, csync):
    """Verify finalization section in pcsm status after pcsm finalize."""
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db = src["testdb"]

    # Failed Indexes
    db["index_failed"].insert_many([
        {"item_id": i, "email": f"item{i}@example.com", "name": f"Item {i}", "amount": round(10.0 + i, 2),
         "created_at": i, **( {"deleted_at": i} if i % 10 == 0 else {})}
        for i in range(100)
    ])
    db["index_failed"].create_index("email", unique=True, name="index_unique")
    db["index_failed"].create_index([("name", pymongo.ASCENDING), ("amount", pymongo.ASCENDING)], name="index_compound")
    db["index_failed"].create_index("deleted_at", sparse=True, name="index_sparse")
    db["index_failed"].create_index("item_id", name="index_single")
    db["index_failed"].create_index("created_at", expireAfterSeconds=86400, name="index_ttl")

    # Successful Indexes
    db["index_passed"].insert_many([{"item_id": i, "value": i} for i in range(100)])
    db["index_passed"].create_index("item_id", name="index_item_id")
    db["index_passed"].create_index("value", name="index_value")

    src.close()

    dst.admin.command({
        "configureFailPoint": "failCommand",
        "mode": "alwaysOn",
        "data": {
            "failCommands": ["createIndexes"],
            "namespace": "testdb.index_failed",
            "errorCode": 14031,
        },
    })
    dst.close()

    status = csync.status()
    assert status["success"], "Failed to retrieve csync status before start"
    assert "finalization" not in status["data"], "finalization should not appear before csync starts"

    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"

    status = csync.status()
    assert status["success"], "Failed to retrieve csync status before finalize"
    assert "finalization" not in status["data"], "finalization should not appear before finalize is called"

    assert csync.finalize(), "Failed to finalize csync"

    status = csync.status()
    assert status["success"], "Failed to retrieve csync status"
    data = status["data"]

    finalization = data.get("finalization", {})
    assert finalization.get("completed") is True, "Finalization did not complete"

    unsuccessful = finalization.get("unsuccessfulIndexes", [])
    assert len(unsuccessful) == 5, f"Expected 5 unsuccessful indexes, got {len(unsuccessful)}: {unsuccessful}"

    unsuccessful_names = {entry["indexName"] for entry in unsuccessful}
    expected_failed = {"index_unique", "index_compound", "index_sparse", "index_single", "index_ttl"}
    assert unsuccessful_names == expected_failed, f"Unexpected unsuccessful index names: {unsuccessful_names}"

    started_at = datetime.fromisoformat(finalization["startedAt"].replace("Z", "+00:00"))
    completed_at = datetime.fromisoformat(finalization["completedAt"].replace("Z", "+00:00"))
    assert started_at < completed_at, f"startedAt {started_at} should be before completedAt {completed_at}"

    for entry in unsuccessful:
        assert entry["namespace"] == "testdb.index_failed", f"Unexpected namespace: {entry['namespace']}"
        assert entry["type"] == "failed", f"Expected type 'failed', got '{entry['type']}'"
        assert "reason" in entry, f"Missing 'reason' in entry: {entry}"

    dst_failed_index_names = {idx["name"] for idx in get_indexes(dst_cluster.connection, "testdb.index_failed")}
    for name in {"index_unique", "index_compound", "index_sparse", "index_single", "index_ttl"}:
        assert name not in dst_failed_index_names, f"{name} should not exist on destination after failed finalization"

    dst_passed_index_names = {idx["name"] for idx in get_indexes(dst_cluster.connection, "testdb.index_passed")}
    for name in {"index_item_id", "index_value"}:
        assert name in dst_passed_index_names, f"{name} should exist on destination after successful finalization"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_pcsm_status_finalization_no_failed_indexes_PCSM_T96(start_cluster, src_cluster, dst_cluster, csync):
    """Verify unsuccessfulIndexes does not appear in finalization status when all indexes succeed."""
    src = pymongo.MongoClient(src_cluster.connection)
    db = src["testdb"]

    db["items"].insert_many([{"item_id": i, "value": i} for i in range(100)])
    db["items"].create_index("item_id", name="index_item_id")
    db["items"].create_index("value", name="index_value")

    src.close()

    status = csync.status()
    assert status["success"], "Failed to retrieve csync status before start"
    assert "finalization" not in status["data"], "finalization should not appear before csync starts"

    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"

    status = csync.status()
    assert status["success"], "Failed to retrieve csync status before finalize"
    assert "finalization" not in status["data"], "finalization section should not appear before finalize is called"

    assert csync.finalize(), "Failed to finalize csync"

    status = csync.status()
    assert status["success"], "Failed to retrieve csync status"
    data = status["data"]

    finalization = data.get("finalization", {})
    assert finalization.get("completed") is True, "Finalization did not complete"
    assert "unsuccessfulIndexes" not in finalization, \
        f"unsuccessfulIndexes should not appear when all indexes succeed: {finalization.get('unsuccessfulIndexes')}"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.timeout(300, func_only=True)
def test_pcsm_status_finalization_retry_clears_failed_indexes_PCSM_T97(start_cluster, src_cluster, dst_cluster, csync):
    """Verify that a second finalize after fixing the index issue clears unsuccessfulIndexes."""
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db = src["testdb"]

    db["items"].insert_many([{"item_id": i, "value": i} for i in range(100)])
    db["items"].create_index("item_id", name="index_item_id")
    db["items"].create_index("value", name="index_value")

    src.close()

    dst.admin.command({
        "configureFailPoint": "failCommand",
        "mode": "alwaysOn",
        "data": {
            "failCommands": ["createIndexes"],
            "namespace": "testdb.items",
            "errorCode": 14031,
        },
    })

    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync on first attempt"

    dst.admin.command({"configureFailPoint": "failCommand", "mode": "off"})
    dst.close()

    assert csync.finalize(), "Failed to finalize csync on second attempt"

    status = csync.status()
    assert status["success"], "Csync status was not successful"
    second_finalization = status["data"].get("finalization", {})
    assert second_finalization.get("completed") is True, "Finalization did not complete on second attempt"
    assert "unsuccessfulIndexes" not in second_finalization, \
        f"unsuccessfulIndexes should be cleared after successful second finalize: {second_finalization.get('unsuccessfulIndexes')}"

    dst_index_names = {idx["name"] for idx in get_indexes(dst_cluster.connection, "testdb.items")}
    for name in {"index_item_id", "index_value"}:
        assert name in dst_index_names, f"{name} should exist on destination after successful second finalize"
