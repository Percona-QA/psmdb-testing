import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading
import random
import string

from datetime import datetime
from cluster import Cluster


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

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
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test1", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(6000, func_only=True)
def test_mixed_pitr(start_cluster,cluster):
    cluster.check_pbm_status()

    for node in cluster.pbm_hosts:
        n = testinfra.get_host("docker://" + node)
        #modified  checkpoints timeouts
        result=n.check_output('mongo -u root -p root --eval \'db.adminCommand({ "setParameter": 1, "wiredTigerEngineRuntimeConfig": "checkpoint=(log_size=1, wait=1)"})\'')

    cluster.make_backup("logical")

    def upsert_1():
        Cluster.log("Starting background upsert 1")
        while upsert:
            document = {
                random.choice(string.ascii_letters): random.randint(0, 100)
            }
            pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_one(document)
        Cluster.log("Stopping background upsert 1")

    def upsert_2():
        Cluster.log("Starting background upsert 2")
        while upsert:
            document = {
                random.choice(string.ascii_letters): random.randint(0, 100)
            }
            pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_one(document)
        Cluster.log("Stopping background upsert 2")

    cluster.enable_pitr()

    upsert=True
    t1 = threading.Thread(target=upsert_1)
    t2 = threading.Thread(target=upsert_2)
    t1.start()
    t2.start()

    try:
        for i in range(10):
            time.sleep(1)
            cluster.make_backup("incremental --base")
            time.sleep(1)
            backup=cluster.make_backup("physical")
            time.sleep(1)
            cluster.make_backup("incremental")
    except AssertionError: 
        upsert=False
        t2.join()
        t1.join()
        assert False

    upsert=False
    t2.join()
    t1.join()

    cluster.disable_pitr()

    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    Cluster.log("Finished successfully")

