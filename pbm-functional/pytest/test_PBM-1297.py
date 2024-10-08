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
def newconfig():
    return { "mongos": "newmongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"newrscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"newrs101"}]},
                            {"_id": "rs2", "members": [{"host":"newrs201"}]}
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
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()
        newcluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600, func_only=True)
def test_logical_pitr_PBM_T253(start_cluster,cluster,newcluster):
    cluster.check_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    #Create the first database during oplog slicing
    client=pymongo.MongoClient(cluster.connection)
    client.admin.command("enableSharding", "test")
    client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
    for i in range(100):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"doc":i})
    time.sleep(30)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(30)
    cluster.disable_pitr()
    time.sleep(10)
    cluster.destroy()

    newcluster.create()
    newcluster.setup_pbm()
    time.sleep(10)
    newcluster.check_pbm_status()
    newcluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(newcluster.connection)["test"]["test"].count_documents({}) == 100
    assert pymongo.MongoClient(newcluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

