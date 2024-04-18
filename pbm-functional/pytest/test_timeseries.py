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
from packaging import version

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]},
                            {"_id": "rs2", "members": [{"host":"rs201"}]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        mongod_version=client.server_info()["version"]
        if version.parse(mongod_version) < version.parse("6.0.0"):
            pytest.skip("Unsupported version for sharded timeseries")
        client.admin.command("enableSharding", "test")
        client.test.create_collection('test1',timeseries={'timeField':'timestamp','metaField': 'data'})
        client.test.create_collection('test2',timeseries={'timeField':'timestamp','metaField': 'data'})
        client.admin.command("shardCollection", "test.test1", key={"timestamp": 1})
        client.test["test2"].create_index([("data",pymongo.HASHED )])
        client.admin.command("shardCollection", "test.test2", key={"data": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600,func_only=True)
def test_logical_PBM_T252(start_cluster,cluster):
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_one({"timestamp": datetime.now(), "data": i})
        pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_one({"timestamp": datetime.now(), "data": i})
        time.sleep(0.1)
    backup=cluster.make_backup("logical")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_logical_without_data(start_cluster,cluster):
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    backup=cluster.make_backup("logical")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    Cluster.log("Finished successfully")
