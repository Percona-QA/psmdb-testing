import concurrent.futures
import os
import time

import pymongo
import pytest

from datetime import datetime
from cluster import Cluster

BALLAST_COUNT = 100_000
BALLAST_PAYLOAD_BYTES = 640

def _seed_ballast(col) -> None:
    for s in range(0, BALLAST_COUNT, 100):
        e = min(s + 100, BALLAST_COUNT)
        col.insert_many(
            [{"idx": i, "changed": 0, "pad": os.urandom(BALLAST_PAYLOAD_BYTES)} for i in range(s, e)],
            ordered=False,
        )
    Cluster.log(f"Seeded {BALLAST_COUNT} docs ({BALLAST_PAYLOAD_BYTES}B/doc)")

@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}]},
        ],
    }

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.setup_pbm("/etc/pbm-fs.conf")
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "trx")
        client.admin.command("shardCollection", "trx.test", key={"idx": 1})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600, func_only=True)
def test_distributed_trx_pitr(start_cluster, cluster):
    """
    Verifies that PITR correctly handles cross-shard distributed transactions
    whose writes span the backup window.

    trx1 is open during the backup:
      - phase 1 writes happen before the backup starts
      - phase 2 writes happen while the backup is running
      - phase 3 writes happen after the backup completes
      trx1 commits before the PITR restore point → all changes must be visible.

    trx2 commits after the PITR restore point → changes must be absent.
    """
    client = pymongo.MongoClient(cluster.connection)
    col = client["trx"]["test"]

    for host in ["rs101", "rs201", "rscfg01"]:
        pymongo.MongoClient(f"mongodb://root:root@{host}/?authSource=admin").admin.command(
            "setParameter", 1, transactionLifetimeLimitSeconds=300
        )

    _seed_ballast(col)

    Cluster.log("Setting up chunk placement: idx 0-499 → rs1, idx 500-99999 → rs2")
    client.admin.command("split", "trx.test", middle={"idx": 500})
    client.admin.command("moveChunk", "trx.test", find={"idx": 100}, to="rs1")
    client.admin.command("moveChunk", "trx.test", find={"idx": 600}, to="rs2")

    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")

    Cluster.log("Opening trx1 (cross-shard, will span the backup window)")
    with client.start_session() as session:
        with session.start_transaction():
            # Phase 1: writes before the backup starts
            col.update_one({"idx": 100}, {"$set": {"changed": 1}}, session=session)
            col.update_one({"idx": 600}, {"$set": {"changed": 1}}, session=session)

            background_backup = concurrent.futures.ThreadPoolExecutor().submit(cluster.make_backup, "logical")

            # Wait until the backup is confirmed actively running
            timeout = time.time() + 60
            while True:
                if cluster.get_status().get("running", {}).get("type") == "backup":
                    break
                assert time.time() < timeout, "Timed out waiting for backup to start running"
                time.sleep(1)
            Cluster.log("trx1 phase 2: backup is running")

            # Phase 2: writes while the backup is in progress
            col.update_one({"idx": 110}, {"$set": {"changed": 1}}, session=session)
            col.update_one({"idx": 610}, {"$set": {"changed": 1}}, session=session)

            assert cluster.get_status().get("running", {}).get("type") == "backup", "Backup completed before phase 2 writes — dataset is too small to test the concurrent scenario"

            background_backup.result()  # block until backup completes
            Cluster.log("trx1 phase 3: backup has completed")

            # Phase 3: writes after the backup completes, before trx1 commits
            col.update_one({"idx": 120}, {"$set": {"changed": 1}}, session=session)
            col.update_one({"idx": 620}, {"$set": {"changed": 1}}, session=session)

    Cluster.log("trx1 committed")

    # Sleep so the restore point timestamp is clearly after trx1 committed.
    time.sleep(10)
    restore_point = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Restore point: " + restore_point)
    time.sleep(5)

    # trx2: cross-shard transaction that commits after the restore point.
    Cluster.log("Running trx2 (cross-shard): idx=200 (rs1) and idx=700 (rs2)")
    with client.start_session() as session:
        with session.start_transaction():
            col.update_one({"idx": 200}, {"$set": {"changed": 1}}, session=session)
            col.update_one({"idx": 700}, {"$set": {"changed": 1}}, session=session)

    cluster.disable_pitr(restore_point)
    cluster.make_restore("--time=" + restore_point, check_pbm_status=True)

    client = pymongo.MongoClient(cluster.connection)
    col = client["trx"]["test"]

    Cluster.log("Checking trx1 (all phases, expect changed=1)")
    for idx in [100, 110, 120, 600, 610, 620]:
        assert col.find_one({"idx": idx})["changed"] == 1, f"idx={idx}: trx1 should be visible after restore"

    Cluster.log("Checking trx2 (committed after restore point, expect changed=0)")
    for idx in [200, 700]:
        assert col.find_one({"idx": idx})["changed"] == 0, f"idx={idx}: trx2 should not be visible after restore"

    Cluster.log("Checking untouched docs on each shard (expect changed=0)")
    for idx in [50, 800]:
        assert col.find_one({"idx": idx})["changed"] == 0, f"idx={idx}: untouched doc should be unchanged after restore"

    assert client["trx"].command("collstats", "test").get("sharded", False), "collection should still be sharded after restore"

    Cluster.log("Finished successfully")

@pytest.mark.timeout(600, func_only=True)
def test_distributed_trx_physical(start_cluster, cluster):
    """
    Verifies that a physical backup correctly handles cross-shard distributed
    transactions relative to the backup window.

    trx1 commits fully before the backup starts → all changes must be visible after restore.
    trx2 commits after the backup ends → changes must be absent after restore.
    """
    client = pymongo.MongoClient(cluster.connection)
    col = client["trx"]["test"]

    for host in ["rs101", "rs201", "rscfg01"]:
        pymongo.MongoClient(f"mongodb://root:root@{host}/?authSource=admin").admin.command(
            "setParameter", 1, transactionLifetimeLimitSeconds=300
        )

    _seed_ballast(col)

    Cluster.log("Setting up chunk placement: idx 0-499 → rs1, idx 500-99999 → rs2")
    client.admin.command("split", "trx.test", middle={"idx": 500})
    client.admin.command("moveChunk", "trx.test", find={"idx": 100}, to="rs1")
    client.admin.command("moveChunk", "trx.test", find={"idx": 600}, to="rs2")

    # trx1: cross-shard transaction that commits before the backup starts.
    Cluster.log("Running trx1 (cross-shard): commits before backup starts")
    with client.start_session() as session:
        with session.start_transaction():
            col.update_one({"idx": 100}, {"$set": {"changed": 1}}, session=session)
            col.update_one({"idx": 600}, {"$set": {"changed": 1}}, session=session)
            col.update_one({"idx": 110}, {"$set": {"changed": 1}}, session=session)
            col.update_one({"idx": 610}, {"$set": {"changed": 1}}, session=session)
    Cluster.log("trx1 committed")

    backup = cluster.make_backup("physical")

    # trx2: cross-shard transaction that commits after the backup ends.
    Cluster.log("Running trx2 (cross-shard): commits after backup ends")
    with client.start_session() as session:
        with session.start_transaction():
            col.update_one({"idx": 200}, {"$set": {"changed": 1}}, session=session)
            col.update_one({"idx": 700}, {"$set": {"changed": 1}}, session=session)
    Cluster.log("trx2 committed")

    cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)

    client = pymongo.MongoClient(cluster.connection)
    col = client["trx"]["test"]

    Cluster.log("Checking trx1 (committed before backup, expect changed=1)")
    for idx in [100, 110, 600, 610]:
        assert col.find_one({"idx": idx})["changed"] == 1, f"idx={idx}: trx1 should be visible after restore"

    Cluster.log("Checking trx2 (committed after backup, expect changed=0)")
    for idx in [200, 700]:
        assert col.find_one({"idx": idx})["changed"] == 0, f"idx={idx}: trx2 should not be visible after restore"

    Cluster.log("Checking untouched docs on each shard (expect changed=0)")
    for idx in [50, 800]:
        assert col.find_one({"idx": idx})["changed"] == 0, f"idx={idx}: untouched doc should be unchanged after restore"

    assert client["trx"].command("collstats", "test").get("sharded", False), "collection should still be sharded after restore"

    Cluster.log("Finished successfully")
