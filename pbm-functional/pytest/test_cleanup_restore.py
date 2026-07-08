import os

import pymongo
import pytest

from cluster import Cluster

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
        client.admin.command("enableSharding", "cleanupdb")
        client.admin.command("shardCollection", "cleanupdb.cleanupcol", key={"idx": 1})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def get_chunk_distribution(client, namespace):
    """Return a dict mapping shard name -> chunk count for a given namespace."""
    chunks = client["config"]["chunks"].find({"ns": namespace}, {"shard": 1, "_id": 0})
    distribution = {}
    for chunk in chunks:
        shard = chunk["shard"]
        distribution[shard] = distribution.get(shard, 0) + 1
    return distribution


@pytest.mark.timeout(300, func_only=True)
def test_cleanup_full_restore_PBM_T358(start_cluster, cluster):
    """
    Verifies that a restore correctly cleans up chunk routing metadata that changed after the backup was taken.
    """
    client = pymongo.MongoClient(cluster.connection)
    col = client["cleanupdb"]["cleanupcol"]

    client.admin.command("balancerStop")

    col.insert_many([{"idx": i} for i in range(2000)])

    backup = cluster.make_backup("logical")
    distribution_before = get_chunk_distribution(client, "cleanupdb.cleanupcol")

    client.admin.command("split", "cleanupdb.cleanupcol", middle={"idx": 1000})
    client.admin.command("moveChunk", "cleanupdb.cleanupcol", find={"idx": 500}, to="rs2")

    cluster.make_restore(backup, check_pbm_status=True)

    client = pymongo.MongoClient(cluster.connection)
    actual = client["cleanupdb"]["cleanupcol"].count_documents({})
    assert actual == 2000, f"Expected 2000 docs after restore, got {actual}"
    assert client["cleanupdb"].command("collstats", "cleanupcol").get("sharded", False)

    distribution_after = get_chunk_distribution(client, "cleanupdb.cleanupcol")
    assert distribution_after == distribution_before, (
        f"Chunk distribution mismatch after restore: before={distribution_before}, after={distribution_after}"
    )
