import pytest
import pymongo
import time
import os
import docker

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
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host":"rs102"},{"host":"rs103"}]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host":"rs202"},{"host":"rs203"}]}
                      ]}


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config, mongod_extra_args="--setParameter logicalSessionRefreshMillis=180000") # 3minutes

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups "
                                    "--set backup.compression=none --out json --wait")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()
        os.system("rm -rf /backups/*")

@pytest.mark.timeout(1200, func_only=True)
def test_disabled_pitr_PBM_T251(start_cluster,cluster):
    cluster.check_pbm_status()
    for i in range(30):
         pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"a": i})
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=3")
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
    time.sleep(30)
    backup="--time=" + pitr + " --base-snapshot=" + backup
    cluster.disable_pitr()
    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 60


    backup=cluster.make_backup("physical")
    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 60

