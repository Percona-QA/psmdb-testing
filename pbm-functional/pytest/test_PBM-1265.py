import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading
import json

from datetime import datetime
from cluster import Cluster

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
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]},
                            {"_id": "rs3", "members": [{"host":"rs301"},{"host": "rs302"},{"host": "rs303" }]}
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
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

#@pytest.mark.timeout(600, func_only=True)
def test_physical_pitr(start_cluster,cluster):
    cluster.check_pbm_status()
    cluster.exec_pbm_cli("config --set pitr.enabled=true --set pitr.compression=none --set pitr.oplogSpanMin=5")
    for i in range(30):
         pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"a": i})
    cluster.make_backup("logical")
    backup=cluster.make_backup("physical")
    for i in range(30):
         pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"b": i})
    timeout = time.time() + 360
    while True:
        status = cluster.get_status()
        for snapshot in status['backups']['snapshot']:
            if snapshot['name'] == backup:
                backup_epoch = snapshot['restoreTo']
        if 'pitrChunks' in status['backups']:
            if 'pitrChunks' in status['backups']['pitrChunks']:
                pitr_epoch = status['backups']['pitrChunks']['pitrChunks'][-1]['range']['end']
                if pitr_epoch > backup_epoch:
                    pitr = datetime.utcfromtimestamp(pitr_epoch).strftime("%Y-%m-%dT%H:%M:%S")
                    Cluster.log("Time for PITR is: " + pitr)
                    break
        if time.time() > timeout:
            assert False
        time.sleep(1)
    time.sleep(60)
    backup="--time=" + pitr + " --base-snapshot=" + backup
    cluster.disable_pitr()
    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 60

    cluster.exec_pbm_cli("config --set pitr.enabled=true --set pitr.compression=none --set pitr.oplogSpanMin=5")
    cluster.make_backup("logical")
    backup=cluster.make_backup("physical")
    cluster.disable_pitr()
    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 60

