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
    def insert_data(db,collection):
        Cluster.log("Starting background insert to " + collection)
        while upsert:
            document = {
                'data': random.randbytes(1024)
            }
            try:
                pymongo.MongoClient(cluster.connection)[db][collection].insert_one(document)
            except BaseException:
                break
        Cluster.log("Stopping background insert to " + collection)
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test1", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        for node in cluster.pbm_hosts:
            n = testinfra.get_host("docker://" + node)
            result=n.check_output('mongo -u root -p root --eval \'db.getSiblingDB("admin").adminCommand({ "setParameter": 1, "wiredTigerEngineRuntimeConfig": "checkpoint=(log_size=1, wait=1),debug_mode=(slow_checkpoint=true)"})\'')
        upsert=True
        t1 = threading.Thread(target=insert_data, args=("test","test1",))
        t2 = threading.Thread(target=insert_data, args=("test","test2",))
        t3 = threading.Thread(target=insert_data, args=("test","test3",))
        t4 = threading.Thread(target=insert_data, args=("test","test4",))
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        upsert=False
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        cluster.destroy()

@pytest.mark.testcase(test_case_key="T246", test_step_key=1)
@pytest.mark.timeout(6000, func_only=True)
def test_incremental_base(start_cluster,cluster):
    cluster.check_pbm_status()
    time.sleep(10)
    for i in range(20):
        time.sleep(1)
        cluster.make_backup("incremental --base")

    Cluster.log("Finished successfully")

@pytest.mark.testcase(test_case_key="T245", test_step_key=1)
@pytest.mark.timeout(6000, func_only=True)
def test_incremental(start_cluster,cluster):
    cluster.check_pbm_status()
    time.sleep(10)
    cluster.make_backup("incremental --base")
    for i in range(20):
        time.sleep(1)
        pymongo.MongoClient(cluster.connection)['test']['test'].insert_one({'data': random.randbytes(1024)})
        backup=cluster.make_backup("incremental")

    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 20
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.testcase(test_case_key="T247", test_step_key=1)
@pytest.mark.timeout(6000, func_only=True)
def test_physical(start_cluster,cluster):
    cluster.check_pbm_status()
    time.sleep(10)
    for i in range(20):
        time.sleep(1)
        cluster.make_backup("physical")

    Cluster.log("Finished successfully")
