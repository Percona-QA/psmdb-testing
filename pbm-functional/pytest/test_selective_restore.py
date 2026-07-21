import os

import pymongo
import pytest

from cluster import Cluster

@pytest.fixture(scope="function")
def config(cluster_configs):
    return cluster_configs

@pytest.fixture(scope="function")
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
        if cluster.layout == "sharded":
            client = pymongo.MongoClient(cluster.connection)
            client.admin.command("enableSharding", "db0")
            client.admin.command("shardCollection", "db0.c01", key={"i": "hashed"})
            client.admin.command("shardCollection", "db0.c03", key={"i": "hashed"})
            client.admin.command("enableSharding", "db1")
            client.admin.command("shardCollection", "db1.c10", key={"r": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def insert_data(client, batch=1):
    for i in range(10):
        val = i + (batch - 1) * 10
        client["db0"]["c00"].insert_one({"i": val})
        client["db0"]["c01"].insert_one({"i": val})
        client["db0"]["c02"].insert_one({"i": val})
        client["db0"]["c03"].insert_one({"i": val})
        client["db1"]["c10"].insert_one({"r": val})
        client["db1"]["c11"].insert_one({"i": val})

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_selective_backup_sharded_T364(start_cluster, cluster):
    """
    Verify the correct collections are restored after a selective restore.
    """
    client = pymongo.MongoClient(cluster.connection)

    insert_data(client, batch=1)

    backup = cluster.make_backup("logical", ns="db0.*")

    insert_data(client, batch=2)

    cluster.make_restore(
        backup,
        restore_opts=["--ns=db0.c00,db0.c01"],
        check_pbm_status=True,
    )

    client = pymongo.MongoClient(cluster.connection)

    assert client["db0"]["c00"].count_documents({}) == 10, "db0.c00 should be restored to backup state"
    assert client["db0"]["c01"].count_documents({}) == 10, "db0.c01 should be restored to backup state"

    assert client["db0"]["c02"].count_documents({}) == 20, "db0.c02 should retain post-backup data"
    assert client["db0"]["c03"].count_documents({}) == 20, "db0.c03 should retain post-backup data"

    assert client["db1"]["c10"].count_documents({}) == 20, "db1.c10 should be unaffected"
    assert client["db1"]["c11"].count_documents({}) == 20, "db1.c11 should be unaffected"