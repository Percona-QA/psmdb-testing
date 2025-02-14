import pytest
import pymongo
import time
import os
import docker
import random
import string
import threading

from datetime import datetime
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]},
                            {"_id": "rs2", "members": [{"host":"rs201"}]},
                            {"_id": "rs3", "members": [{"host":"rs301"}]}
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
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none --out json")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        time.sleep(5)
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()

def insert_docs(connection,duration):
    client=pymongo.MongoClient(connection)
    timeout = time.time() + duration
    while True:
        id = 'user' + str(random.randint(10**8,10**9-1))
        data = random.randbytes(10*1024)
        client['test']['test'].update_one({ '_id': id }, { "$set": { 'data': data}}, upsert=True)
        if time.time() > timeout:
            break

@pytest.mark.timeout(600,func_only=True)
def test_load_shard_collection_pitr_PBM_T289(start_cluster,cluster):
    cluster.check_pbm_status()
    cluster.make_backup('logical')
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    Cluster.log("Start loading data")
    threads = 200
    duration = 120
    background_insert = [None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs,args=(cluster.connection,duration,))
        background_insert[i].start()
    for i in range(threads):
        background_insert[i].join()
    Cluster.log("Stop loading data")
    client=pymongo.MongoClient(cluster.connection)
    client.admin.command("enableSharding", "test")
    result=client.admin.command("shardCollection", "test.test", key={"_id": 1})
    Cluster.log("Shard collection test.test:")
    Cluster.log(result)
    cluster.make_backup('logical')
    time.sleep(20)
    expected_docs_count_pitr = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Expected count documents for PITR: " + str(expected_docs_count_pitr))
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup= "--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr()
    time.sleep(30)
    cluster.make_restore(backup,make_resync=False,check_pbm_status=True)

    try:
        actual_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        assert actual_docs_count == expected_docs_count_pitr
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Mongos failure - pymongo assertion error: " + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")


