import os
import time
import threading
import docker
import pymongo
import pytest
from packaging import version

from cluster import Cluster

documents_post_snapshot = [{"x": 10001, "b": 5}, {"x": 10002, "c": 6}]

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
        cluster.exec_pbm_cli("config --set compression=none")
        Cluster.log(client.admin.command({"transitionFromDedicatedConfigServer": 1}))
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.data", key={"x": 1})
        client.admin.command("split", "test.data", middle={"x": 0})
        client.admin.command("moveChunk", "test.data", find={"x": 1}, to="config")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(900, func_only=True)
def test_logical_PBM_T306(start_cluster, cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)

    client["test"]["data"].insert_many([{"x": (i + 1), "pad": "x" * 200} for i in range(10000)])

    backup_result = {"backup_full": None}

    def run_backup():
        backup_result["backup_full"] = cluster.make_backup("logical")

    backup_thread = threading.Thread(target=run_backup)
    backup_thread.start()

    target_log_pattern = 'dump collection "test.data" done'
    max_wait_time = 300
    start_time = time.time()
    log_found = False

    while backup_thread.is_alive() and not log_found:
        if time.time() - start_time > max_wait_time:
            Cluster.log("Timeout waiting for log pattern")
            break

        result = cluster.exec_pbm_cli("logs --tail=50")
        if target_log_pattern in result.stdout:
            client["test"]["data"].insert_many(documents_post_snapshot)
            log_found = True

    backup_thread.join()
    assert log_found, f"Targeted log {target_log_pattern} not found"

    backup_name = backup_result["backup_full"]
    cluster.make_restore(backup_name, restore_opts=["--ns=test.data"], restart_cluster=False, check_pbm_status=True)

    document_count = client["test"]["data"].count_documents({})
    assert client["test"]["data"].count_documents({"x": {"$in": [10001, 10002]}}) == 2
    assert document_count == 10002, f"Expected {10000 + len(documents_post_snapshot)} documents, got {document_count} instead"
