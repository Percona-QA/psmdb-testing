import os
import time
import threading
import docker
import pymongo
import pytest
from packaging import version

from cluster import Cluster

documents_pre_backup = [{"a": 1}]
documents_during_backup = [{"b": 2}, {"c": 3}]

@pytest.fixture(scope="package")
def mongod_version():
    return docker.from_env().containers.run(
        image="replica_member/local",
        remove=True,
        command="bash -c 'mongod --version | head -n1 | sed \"s/db version v//\"'",
    ).decode("utf-8", errors="replace").strip()

@pytest.fixture(scope="package")
def config(mongod_version):
    if version.parse(mongod_version) < version.parse("8.0.0"):
        pytest.skip("Unsupported version for config shards (MongoDB < 8.0)")

    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}]}
        ],
    }

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")

        cluster.create()
        cluster.setup_azurite()
        cluster.setup_pbm("/etc/pbm-fs.conf")

        client = pymongo.MongoClient(cluster.connection)
        Cluster.log(client.admin.command({"transitionFromDedicatedConfigServer": 1}))

        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.data", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(900, func_only=True)
def test_logical_backup_restore_config_shard(start_cluster, cluster):
    cluster.check_pbm_status()

    client = pymongo.MongoClient(cluster.connection)
    client["test"]["data"].insert_many(documents_pre_backup)
    backup_result = {"backup_full": None}

    def run_backup():
        backup_result["backup_full"] = cluster.make_backup("logical")

    backup_thread = threading.Thread(target=run_backup)
    backup_thread.start()

    target_log_pattern = "test.data"
    max_wait_time = 300
    start_time = time.time()
    log_found = False

    while backup_thread.is_alive() and not log_found:
        if time.time() - start_time > max_wait_time:
            Cluster.log("Timeout waiting for log pattern")
            break

        result = cluster.exec_pbm_cli("logs --tail=50")

        if target_log_pattern in result.stdout:
            log_found = True
            client["test"]["data"].insert_many(documents_during_backup)
    backup_thread.join()

    assert log_found, f"Targeted log {target_log_pattern} not found"
    backup_name = backup_result["backup_full"]
    cluster.make_restore(backup_name, restart_cluster=True, check_pbm_status=True)
    document_count = client["test"]["data"].count_documents({})
    assert document_count == 3, f"Expected 3 documents (1 original + 2 during backup), got {document_count}"
