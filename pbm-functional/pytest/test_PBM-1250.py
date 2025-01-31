import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import random
import string
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
        Cluster.log("Set autoMergerIntervalSecs to 30 sec")
        result = pymongo.MongoClient('mongodb://root:root@rscfg01:27017?authSource=admin&replicaSet=rscfg').admin.command( { "setParameter": 1, "autoMergerIntervalSecs": 30 } )
        print(result)
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": 1})
        Cluster.log("Set chunksize to 10Mb")
        result = client['config']['settings'].update_one({ '_id': 'chunksize' },{ "$set": { '_id': 'chunksize', 'value': 10 } },upsert=True)
        print(result)
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600,func_only=True)
def test_chunks_migration(start_cluster,cluster):
    def insert_docs():
        client=pymongo.MongoClient(cluster.connection)
        timeout = time.time() + 90
        while True:
            #simulate the data produced by YCSB
            id = 'user' + str(random.randint(10**12,10**13-1))
            data = random.randbytes(10*1024)
            client['test']['test'].insert_one({'_id':id,'data': data})
            if time.time() > timeout:
                break

    Cluster.log("Start inserting docs in the background")
    threads = 100
    background_insert=[None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs)
        background_insert[i].start()

    client=pymongo.MongoClient(cluster.connection)
    cluster.check_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    Cluster.log("Check chunks distribution after the backup")
    for chunks in client['config']['chunks'].find():
        print(chunks)
    time.sleep(30)

    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup= "--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)

    Cluster.log("Check chunks distribution after 30 seconds")
    for chunks in client['config']['chunks'].find():
        print(chunks)

    for i in range(threads):
        background_insert[i].join()

    cluster.disable_pitr()
    time.sleep(5)
    Cluster.log("Check chunks distribution before the restore")
    for chunks in client['config']['chunks'].find():
        print(chunks)
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
