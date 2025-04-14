import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import random

from datetime import datetime
from cluster import Cluster

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host":"rscfg02"},{"host":"rscfg03"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host":"rs102"},{"host":"rs103"}]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


#the dirty hack which allows create parallel agents for different nodes from the orginal cluster, see pbm_mongodb_uri param
wrong_rs101_cluster = Cluster({ "_id": "wrong_rs101", "members": [{"host":"wrong_rs101"}]},pbm_mongodb_uri="mongodb://pbm:pbmpass@rs101:27017/?authSource=admin")
wrong_rs102_cluster = Cluster({ "_id": "wrong_rs102", "members": [{"host":"wrong_rs102"}]},pbm_mongodb_uri="mongodb://pbm:pbmpass@rs102:27017/?authSource=admin")
wrong_rs103_cluster = Cluster({ "_id": "wrong_rs103", "members": [{"host":"wrong_rs103"}]},pbm_mongodb_uri="mongodb://pbm:pbmpass@rs103:27017/?authSource=admin")
wrong_rscfg01_cluster = Cluster({ "_id": "wrong_rscfg01", "members": [{"host":"wrong_rscfg01"}]},pbm_mongodb_uri="mongodb://pbm:pbmpass@rscfg01:27017/?authSource=admin")
wrong_rscfg02_cluster = Cluster({ "_id": "wrong_rscfg02", "members": [{"host":"wrong_rscfg02"}]},pbm_mongodb_uri="mongodb://pbm:pbmpass@rscfg02:27017/?authSource=admin")
wrong_rscfg03_cluster = Cluster({ "_id": "wrong_rscfg03", "members": [{"host":"wrong_rscfg03"}]},pbm_mongodb_uri="mongodb://pbm:pbmpass@rscfg03:27017/?authSource=admin")


@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        wrong_rs101_cluster.destroy()
        wrong_rs102_cluster.destroy()
        wrong_rs103_cluster.destroy()
        wrong_rscfg01_cluster.destroy()
        wrong_rscfg02_cluster.destroy()
        wrong_rscfg03_cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        coll = "test.test"
        client.admin.command("shardCollection", coll, key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        wrong_rs101_cluster.destroy()
        wrong_rs102_cluster.destroy()
        wrong_rs103_cluster.destroy()
        wrong_rscfg01_cluster.destroy()
        wrong_rscfg02_cluster.destroy()
        wrong_rscfg03_cluster.destroy()
        cluster.destroy()

def test_STR_PBM_T292(start_cluster,cluster):
    coll = "test"
    for j in range(10):
        pymongo.MongoClient(cluster.connection)['test'][coll].insert_one({'data': random.randbytes(1024*1024)})
    base_backup = cluster.make_backup("incremental --base")

    # the issue is reproducible when the parallel agent is connected to the primary node of the shard,
    # if connect to secondary the restore will pass with partlyDone status
    wrong_rs101_cluster.create()

    # try restore multiple times because we need that parallel agent starts the restore first,
    # if original agent starts first we will have working cluster after the restore so we can retry
    for i in range(5):
        try:
            cluster.make_restore(base_backup,timeout=300, restart_cluster=True, check_pbm_status=True)
        except AssertionError as e:
            # Get the logs from pbm-agent on rs101, by default mongo log goes to stdout, pbm log to stderr
            rs101_logs = docker.from_env().containers.get('rs101').logs(stdout=False).decode("utf-8", errors="replace")
            assert False, str(e) + "\n Logs from rs101:\n" + rs101_logs
