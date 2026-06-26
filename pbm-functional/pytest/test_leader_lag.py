import json
import threading
import time

import pymongo
import pytest
import testinfra

from cluster import Cluster


@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}]},
        ],
    }


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy(cleanup_backups=True)
        cluster.create()
        cluster.setup_pbm()
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.data", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(600, func_only=True)
def test_csrs_agent_picks_up_backup_after_late_start(start_cluster, cluster):
    """Verify a CSRS agent paused before backup is issued can pick up and complete the backup on resume (PBM-635 LeaderLag)"""
    client = pymongo.MongoClient(cluster.connection)

    for start in range(0, 20000, 1000):
        client["test"]["data"].insert_many([{"x": i, "pad": "x" * 500} for i in range(start, start + 1000)])

    n = testinfra.get_host("docker://rscfg01")
    n.check_output("kill -STOP $(pgrep pbm-agent)")

    backup_result = {}

    def run_backup():
        result = cluster.exec_pbm_cli("backup --type=logical --out=json")
        backup_result["rc"] = result.rc
        backup_result["stdout"] = result.stdout

    backup_thread = threading.Thread(target=run_backup)
    backup_thread.start()

    try:
        time.sleep(5)
        n.check_output("kill -CONT $(pgrep pbm-agent)")
        Cluster.log("CSRS agent resumed, waiting for backup to complete")
    except Exception:
        n.check_output("kill -CONT $(pgrep pbm-agent)")
        raise

    backup_thread.join(timeout=300)
    assert not backup_thread.is_alive(), "Backup command is still running after timeout"
    assert backup_result.get("rc") == 0, f"Backup command failed: {backup_result.get('stdout')}"

    backup_name = json.loads(backup_result["stdout"])["name"]

    timeout = time.time() + 300
    while True:
        status = cluster.get_status()
        snapshots = status.get("backups", {}).get("snapshot", [])
        backup = next((s for s in snapshots if s.get("name") == backup_name), None)
        if backup and backup.get("status") == "done":
            break
        assert backup is None or backup.get("status") != "error", \
            f"Backup failed unexpectedly: {backup.get('err', '')}"
        assert time.time() < timeout, "Timed out waiting for backup to complete"
        time.sleep(3)

    timeout = time.time() + 60
    while True:
        if not cluster.get_status().get("running"):
            break
        assert time.time() < timeout, "Timed out waiting for cluster to become idle before resync"
        time.sleep(2)

    cluster.make_resync()

    snapshots = cluster.get_status().get("backups", {}).get("snapshot", [])
    assert any(s.get("name") == backup_name for s in snapshots), "Backup disappeared from list after resync"

    client.drop_database("test")
    cluster.make_restore(backup_name, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["data"].count_documents({}) == 20000