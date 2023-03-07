import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker

from datetime import datetime
from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]}
                      ]}

@pytest.fixture(scope="package")
def newconfig():
    return { "mongos": "newmongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"newrscfg01"},{"host": "newrscfg02"},{"host": "newrscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"newrs101"},{"host": "newrs102"},{"host": "newrs103" }]},
                            {"_id": "rs2", "members": [{"host":"newrs201"},{"host": "newrs202"},{"host": "newrs203" }]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="package")
def newcluster(newconfig):
    return Cluster(newconfig)

@pytest.fixture(scope="function")
def start_cluster(cluster,newcluster,request):
    try:
        cluster.destroy()
        newcluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        newcluster.create()
        newcluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            newcluster.get_logs()
        cluster.destroy()
        newcluster.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_logical(start_cluster,cluster,newcluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("logical")
    cluster.destroy()

    newcluster.make_resync()
    newcluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(newcluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newcluster.connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_physical(start_cluster,cluster,newcluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    cluster.destroy()

    newcluster.make_resync()
    newcluster.make_restore(backup,restart_cluster=True, make_resync=True, check_pbm_status=True)
    assert pymongo.MongoClient(newcluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newcluster.connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_incremental(start_cluster,cluster,newcluster):
    cluster.check_pbm_status()
    cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("incremental")
    cluster.destroy()

    newcluster.make_resync()
    newcluster.make_restore(backup,restart_cluster=True, make_resync=True, check_pbm_status=True)
    assert pymongo.MongoClient(newcluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newcluster.connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

