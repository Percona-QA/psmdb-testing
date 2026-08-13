import os

import pymongo
import pytest
from bson.binary import Binary
from cluster import Cluster

import docker


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.jenkins
@pytest.mark.parametrize("backup_type", ["logical", "physical"])
@pytest.mark.timeout(400, func_only=True)
def test_PBM_1799_PBM_T368(start_cluster, cluster, backup_type):
    """Verify restore from a GCS SDK-client backup with a split part-object succeeds"""
    cluster.setup_pbm(file="/etc/gcs.conf")
    client = pymongo.MongoClient(cluster.connection)
    mongod_version = client.server_info()["version"]
    major_ver = "".join(mongod_version.split(".")[:2])
    unique_prefix = f"pbm1799/{major_ver}-{backup_type}"

    result = cluster.exec_pbm_cli(
        f'config --set storage.gcs.prefix={unique_prefix} '
        f'--set storage.gcs.maxObjSizeGB=1 --out json -w')
    assert result.rc == 0, result.stdout + result.stderr
    cluster.check_pbm_status()
    result = cluster.exec_pbm_cli("config")
    Cluster.log("Current PBM config:\n" + result.stdout)

    total_docs = (1200 * 1024) // 10
    for i in range(0, total_docs, 1000):
        batch = [{"_id": i + j, "payload": Binary(os.urandom(10 * 1024))}
                for j in range(min(1000, total_docs - i))]
        client["test"]["data"].insert_many(batch)
    Cluster.log(f"Inserted {total_docs} documents (~1.2GB) into test.data")

    backup = cluster.make_backup(backup_type)

    client.drop_database("test")

    if backup_type == "logical":
        cluster.make_restore(backup, timeout=900, check_pbm_status=True)
    else:
        cluster.make_restore(backup, timeout=900, restart_cluster=True, check_pbm_status=True)

    assert client["test"]["data"].count_documents({}) == total_docs
    for i in (0, total_docs // 2, total_docs - 1):
        assert client["test"]["data"].find_one({"_id": i})