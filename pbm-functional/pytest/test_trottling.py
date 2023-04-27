import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading

from datetime import datetime
from cluster import Cluster

documents = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return {"mongos": "mongos",
            "configserver":
            {"_id": "rscfg", "members": [{"host": "rscfg01"}, {
                "host": "rscfg02"}, {"host": "rscfg03"}]},
            "shards": [
                {"_id": "rs1", "members": [{"host": "rs101"}, {
                    "host": "rs102"}, {"host": "rs103"}]},
                {"_id": "rs2", "members": [{"host": "rs201"}, {
                    "host": "rs202"}, {"host": "rs203"}]}
            ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        result = cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio")
        Cluster.log("Setup PBM with minio storage with trottling:\n" + result.stdout)
        assert result.rc == 0
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command(
            "shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command(
            "shardCollection", "test.test2", key={"_id": "hashed"})
        client.admin.command(
            "shardCollection", "test.test3", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(300, func_only=True)
def test_logical(start_cluster, cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("logical")
    result = pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")

"""
@pytest.mark.timeout(300, func_only=True)
def test_physical(start_cluster, cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("physical")
    result = pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup, restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")

@pytest.mark.timeout(300, func_only=True)
def test_incremental(start_cluster, cluster):
    cluster.check_pbm_status()
    cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("incremental")
    result = pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup, restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")
"""
