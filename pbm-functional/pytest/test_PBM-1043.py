import pytest
import pymongo
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
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103"}]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203"}]}
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

@pytest.mark.timeout(600,func_only=True)
def test_logical_PBM_T255(start_cluster,cluster):
    def insert_docs():
        client=pymongo.MongoClient(cluster.connection)
        for i in range(200):
            client['test']['test'].insert_one({"doc":i})
            time.sleep(1)

    cluster.check_pbm_status()
    cluster.make_backup("logical")
    cluster.exec_pbm_cli("config --file=/etc/pbm-1043.conf")
    time.sleep(60)
    Cluster.log("Check if PITR is running")
    if not cluster.check_pitr():
        logs=cluster.exec_pbm_cli("logs -sD -t0")
        assert False, logs.stdout


    Cluster.log("Start inserting docs in the background")
    background_insert = threading.Thread(target=insert_docs)
    background_insert.start()
    time.sleep(60)
    Cluster.log("Check if oplog slicer has been started on the nodes with the highest priorities")
    logs=cluster.exec_pbm_cli("logs -sD -t0 -e pitr")
    assert '[rscfg/rscfg03:27017] [pitr] created chunk' in logs.stdout
    assert '[rs1/rs103:27017] [pitr] created chunk' in logs.stdout
    assert '[rs2/rs203:27017] [pitr] created chunk' in logs.stdout

    nrs103=testinfra.get_host("docker://rs103")
    nrs203=testinfra.get_host("docker://rs203")

    Cluster.log("Oplog slicer should survive if either mongo or pbm-agent is unavailable")
    nrs103.check_output('supervisorctl stop mongod')
    nrs203.check_output('supervisorctl stop pbm-agent')

    time.sleep(60)

    Cluster.log("Start pbm-agent and mongod")
    nrs103.check_output('supervisorctl start mongod')
    nrs203.check_output('supervisorctl start pbm-agent')

    time.sleep(60)
    background_insert.join()

    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 200
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(60)
    cluster.disable_pitr()
    pymongo.MongoClient(cluster.connection).drop_database('test')
    status=cluster.exec_pbm_cli("status")
    Cluster.log(status.stdout)
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 200
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    time.sleep(60)
    Cluster.log("PITR must be disabled after the restore")
    assert not cluster.check_pitr()
    cluster.check_pbm_status()
