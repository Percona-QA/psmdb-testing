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
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600,func_only=True)
@pytest.mark.parametrize('restore_base',['full','selective'])
@pytest.mark.parametrize('restore_pitr',['full','selective'])
def test_drop_pitr(start_cluster,cluster,restore_base,restore_pitr):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    client.admin.command({ "enableSharding": "test", "primaryShard": "rs1" })
    for i in range(10):
        client["test"]["test"].insert_one({"key": i, "data": i})
    full_backup = cluster.make_backup("logical")
    selective_backup = cluster.make_backup("logical --ns=test.test")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.05")
    time.sleep(4)
    client.drop_database("test")
    client.admin.command({ "enableSharding": "test", "primaryShard": "rs2" })
    for i in range(10):
        client["test"]["test"].insert_one({"key": i + 10, "data": i + 10})
    time.sleep(4)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    pitr = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(4)
    cluster.disable_pitr()
    restore_type = restore_base + '_' + restore_pitr
    match restore_type:
        case 'full_full':
            backup = " --base-snapshot=" + full_backup + pitr
        case 'selective_full':
            backup = " --base-snapshot=" + selective_backup + pitr
        case 'full_selective':
            backup = " --base-snapshot=" + full_backup + pitr + " --ns=test.test"
        case 'selective_selective':
            backup = " --base-snapshot=" + selective_backup + pitr + " --ns=test.test"
    cluster.make_restore(backup)
    assert client["test"]["test"].count_documents({}) == 10
    for i in range(10):
        assert client["test"]["test"].find_one({"key": i + 10, "data": i + 10})
