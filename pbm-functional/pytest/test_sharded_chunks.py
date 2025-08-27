import pytest
import pymongo
import time
import os
import docker
import random
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
        cluster.create()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.setup_pbm("/etc/pbm-fs.conf")
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": 1})
        Cluster.log("Set chunksize to 16Mb")
        result = client['config']['settings'].update_one({ '_id': 'chunksize' }, { "$set": { '_id': 'chunksize', 'value': 16 }}, upsert=True)
        print(result)
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

def insert_docs(connection,duration):
    client=pymongo.MongoClient(connection)
    timeout = time.time() + duration
    while True:
        id = 'user' + str(random.randint(10**8,10**9-1))
        data = random.randbytes(10*1024)
        client['test']['test'].update_one({ '_id': id }, { "$set": { 'data': data}}, upsert=True)
        if time.time() > timeout:
            break

@pytest.mark.timeout(360,func_only=True)
def test_load_chunks_migration_pitr_PBM_T286(start_cluster,cluster):
    cluster.make_backup('logical')
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    threads = 100
    Cluster.log("Start inserting docs in the background with " + str(threads) + " threads ")
    background_insert = [None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs,args=(cluster.connection,120,))
        background_insert[i].start()
    time.sleep(60)

    expected_docs_count_pitr = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Expected count documents for PITR: " + str(expected_docs_count_pitr))
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup= "--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    Cluster.log("Wait for background threads are finished")
    for i in range(threads):
        background_insert[i].join()
    Cluster.log("Background threads are finished")
    time.sleep(5)
    cluster.disable_pitr()

    time.sleep(5)
    cluster.make_restore(backup,make_resync=False,check_pbm_status=True)

    try:
        actual_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        assert actual_docs_count >= expected_docs_count_pitr
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Mongos failure - pymongo assertion error: " + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")


@pytest.mark.timeout(180,func_only=True)
def test_load_chunks_migration_base_PBM_T285(start_cluster,cluster):
    threads = 100
    Cluster.log("Start inserting docs in the background with " + str(threads) + " threads ")
    background_insert = [None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs,args=(cluster.connection,60,))
        background_insert[i].start()
    time.sleep(30)

    backup=cluster.make_backup("logical")

    expected_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Expected count documents for the backup: " + str(expected_docs_count))

    Cluster.log("Wait for background threads are finished")
    for i in range(threads):
        background_insert[i].join()
    Cluster.log("Background threads are finished")

    time.sleep(5)
    cluster.make_restore(backup,make_resync=False,check_pbm_status=True)

    try:
        actual_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        assert actual_docs_count <= expected_docs_count
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Mongos failure - pymongo assertion error: " + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
