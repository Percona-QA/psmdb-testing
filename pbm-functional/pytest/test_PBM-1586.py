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


@pytest.mark.timeout(300, func_only=True)
def test_wait_returns_error_on_agent_crash_PBM_T320(start_cluster, cluster):
    """Verify pbm backup --wait exits after 30 seconds when the CSRS agent crashes mid-backup"""
    client = pymongo.MongoClient(cluster.connection)

    for start in range(0, 20000, 1000):
        client["test"]["data"].insert_many([{"x": i, "pad": "x" * 500} for i in range(start, start + 1000)])

    def run_backup():
        cluster.exec_pbm_cli("backup --type=logical --wait")

    backup_thread = threading.Thread(target=run_backup)
    backup_thread.start()

    n = testinfra.get_host("docker://rscfg01")
    try:
        Cluster.log("Waiting for backup status to show running")
        timeout = time.time() + 120
        while True:
            status = cluster.get_status()
            snapshots = status.get("backups", {}).get("snapshot", [])
            if snapshots and status.get("running", {}).get("type") == "backup":
                break
            assert time.time() < timeout, "Timed out waiting for backup to start"
            time.sleep(1)

        time.sleep(3)  # Let agent finish its current write before freezing
        n.check_output("kill -STOP $(pgrep pbm-agent)")

        backup_thread.join(timeout=60)
    finally:
        try:
            n.check_output("kill -CONT $(pgrep pbm-agent)")
        except Exception:
            pass

    if backup_thread.is_alive():
        pytest.fail("PBM backup is hanging after 60 seconds despite PBM Agent being down")
