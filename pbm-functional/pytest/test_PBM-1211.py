import pytest
import pymongo
import time
import threading

from datetime import datetime
from cluster import Cluster

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
        result = cluster.exec_pbm_cli('config --file=/etc/pbm-1211.conf --out=json')
        assert result.rc == 0
        Cluster.log("Setup PBM:\n" + result.stdout)
        time.sleep(5)
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(900,func_only=True)
@pytest.mark.parametrize('backup_type',['logical','physical'])
def test_pitr_PBM_T268(start_cluster,cluster,backup_type):
    def insert_docs():
        client=pymongo.MongoClient(cluster.connection)
        for i in range(1500):
            client['test']['test'].insert_one({"doc":i})
            time.sleep(0.1)

    cluster.check_pbm_status()
    base_backup=cluster.make_backup(backup_type)
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    Cluster.log("Start inserting docs in the background")
    background_insert = threading.Thread(target=insert_docs)
    background_insert.start()
    time.sleep(60)
    Cluster.log("Check if PITR is running")
    if not cluster.check_pitr():
        logs=cluster.exec_pbm_cli("logs -sD -t0")
        assert False, logs.stdout
    time.sleep(60)
    background_insert.join()
    time.sleep(30)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1500
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Time for PITR is " + pitr)
    time.sleep(60)
    cluster.disable_pitr()
    pymongo.MongoClient(cluster.connection).drop_database('test')
    backup="--time=" + pitr
    if backup_type == 'logical':
        cluster.make_restore(backup, check_pbm_status=True)
    else:
        cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)
    time.sleep(60)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1500
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_incremental_PBM_T269(start_cluster,cluster):
    cluster.check_pbm_status()
    cluster.make_backup("incremental --base")
    client=pymongo.MongoClient(cluster.connection)
    for i in range(1500):
        client['test']['test'].insert_one({"doc":i})
        time.sleep(0.1)
    backup = cluster.make_backup("incremental")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1500
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
