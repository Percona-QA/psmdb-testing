import os
import time
from datetime import datetime

import docker
import pymongo
import pytest
from packaging import version

from cluster import Cluster


documents = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]


@pytest.fixture(scope="package")
def mongod_version():
    # Use the already-built image to detect the mongod version inside it.
    # (Same approach as pbm-functional/pytest/test_PBM-1355.py)
    return docker.from_env().containers.run(
        image="replica_member/local",
        remove=True,
        command="bash -c 'mongod --version | head -n1 | sed \"s/db version v//\"'",
    ).decode("utf-8", errors="replace").strip()


@pytest.fixture(scope="package")
def config(mongod_version):
    # Config shard / embedded config server is supported in MongoDB 8.0+
    if version.parse(mongod_version) < version.parse("8.0.0"):
        pytest.skip("Unsupported version for config shards (MongoDB < 8.0)")

    # This is still a *normal* sharded cluster definition (dedicated config RS).
    # We convert it into a "config shard" (often referred to as embedded config server)
    # by running transitionFromDedicatedConfigServer via mongos after cluster.create().
    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}, {"host": "rscfg02"}, {"host": "rscfg03"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}]},
        ],
    }


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


@pytest.fixture(
    scope="function",
    params=[
        "/etc/pbm-fs.conf",
        "/etc/pbm-aws-provider.conf",
        "/etc/pbm-minio-provider.conf",
        "/etc/pbm-azurite.conf",
    ],
)
def start_cluster(cluster, request):
    pbm_config = request.param
    try:
        cluster.destroy()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")

        cluster.create()
        cluster.setup_azurite()
        cluster.setup_pbm(pbm_config)

        client = pymongo.MongoClient(cluster.connection)
        # Convert the dedicated config server RS into a config shard
        # (this is the "embedded config server" setup for MongoDB 8.0+).
        Cluster.log(client.admin.command({"transitionFromDedicatedConfigServer": 1}))

        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test3", key={"_id": "hashed"})
        yield pbm_config
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(900, func_only=True)
def test_logical_backup_restore_config_shard(start_cluster, cluster):
    cluster.check_pbm_status()

    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many(documents)
    client["test"]["test1"].insert_many(documents)

    backup_full = cluster.make_backup("logical")
    client.drop_database("test")

    cluster.make_restore(backup_full, check_pbm_status=True)
    assert client["test"]["test1"].count_documents({}) == len(documents)
    assert client["test"].command("collstats", "test1").get("sharded", True) is False
    assert client["test"]["test"].count_documents({}) == len(documents)
    assert client["test"].command("collstats", "test").get("sharded", False)

    Cluster.log("Finished successfully at " + datetime.utcnow().isoformat() + "Z")
