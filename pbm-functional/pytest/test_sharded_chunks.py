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
from packaging import version

@pytest.fixture(scope="package")
def mongod_version():
    return docker.from_env().containers.run(
                    image='replica_member/local',
                    remove=True,
                    command='bash -c \'mongod --version | head -n1 | sed "s/db version v//"\''
          ).decode("utf-8", errors="replace")

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
def start_cluster(cluster,mongod_version,request):
    try:
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none --out json")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
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
        id = 'user' + str(random.randint(10**5,10**6-1))
        data = random.randbytes(10*1024)
        client['test']['test'].update_one({ '_id': id }, { "$set": { 'data': data}}, upsert=True)
        if time.time() > timeout:
            break


@pytest.mark.timeout(120,func_only=True)
def test_load_merge_chunks_PBM_T287(start_cluster,cluster,mongod_version):
    if version.parse(mongod_version) < version.parse("7.0.0"):
        pytest.skip("Unsupported version for autoMerger")

    Cluster.log("Set minSnapshotHistoryWindowInSeconds and transactionLifetimeLimitSeconds to 10 sec")
    for conn in ['rscfg','rs1','rs2','rs3']:
        result = pymongo.MongoClient('mongodb://root:root@'+conn+'01:27017?authSource=admin&replicaSet='+conn).admin.command( { "setParameter": 1, "transactionLifetimeLimitSeconds": 10 } )
        result = pymongo.MongoClient('mongodb://root:root@'+conn+'01:27017?authSource=admin&replicaSet='+conn).admin.command( { "setParameter": 1, "minSnapshotHistoryWindowInSeconds": 10 } )
        print(result)
    Cluster.log("Set autoMergerIntervalSecs to 10 sec")
    result = pymongo.MongoClient('mongodb://root:root@rscfg01:27017?authSource=admin&replicaSet=rscfg').admin.command( { "setParameter": 1, "autoMergerIntervalSecs": 10 } )
    print(result)

    threads = 100
    Cluster.log("Start inserting docs in the background with " + str(threads) + " threads ")
    background_insert = [None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs,args=(cluster.connection,60,))
        background_insert[i].start()

    time.sleep(5)
    cluster.check_pbm_status()
    cluster.make_backup('logical')
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(10)
    expected_docs_count_pitr = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Expected count documents for PITR: " + str(expected_docs_count_pitr))
    time.sleep(5)
    Cluster.log("Check chunks distribution during PITR")
    client=pymongo.MongoClient(cluster.connection)
    for chunks in client['config']['chunks'].find():
        print(chunks)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup= "--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    Cluster.log("Wait for background threads are finished")
    for i in range(threads):
        background_insert[i].join()
    Cluster.log("Background threads are finished")

    time.sleep(10)
    Cluster.log("Check chunks distribution before the restore")
    client=pymongo.MongoClient(cluster.connection)
    for chunks in client['config']['chunks'].find():
        print(chunks)
    cluster.make_restore(backup,check_pbm_status=True)

    try:
        actual_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        assert actual_docs_count >= expected_docs_count_pitr
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Mongos failure - pymongo assertion error: " + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_load_chunks_migration_pitr_PBM_T286(start_cluster,cluster):
    threads = 100
    Cluster.log("Start inserting docs in the background with " + str(threads) + " threads ")
    background_insert = [None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs,args=(cluster.connection,90,))
        background_insert[i].start()
    time.sleep(5)

    cluster.check_pbm_status()
    cluster.make_backup('logical')
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(20)
    expected_docs_count_pitr = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Expected count documents for PITR: " + str(expected_docs_count_pitr))
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup= "--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)

    Cluster.log("Check chunks distribution after 30 seconds")
    for chunks in client['config']['chunks'].find():
        print(chunks)

    Cluster.log("Wait for background threads are finished")
    for i in range(threads):
        background_insert[i].join()
    Cluster.log("Background threads are finished")
    time.sleep(5)
    cluster.disable_pitr()
    time.sleep(5)
    Cluster.log("Check chunks distribution before the restore")
    for chunks in client['config']['chunks'].find():
        print(chunks)
    total_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Total count documents: " + str(total_docs_count))

    cluster.make_restore(backup,check_pbm_status=True)

    try:
        actual_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        assert actual_docs_count >= expected_docs_count_pitr
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Mongos failure - pymongo assertion error: " + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")


@pytest.mark.timeout(120,func_only=True)
def test_load_chunks_migration_base_PBM_T285(start_cluster,cluster):
    threads = 100
    Cluster.log("Start inserting docs in the background with " + str(threads) + " threads ")
    background_insert = [None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs,args=(cluster.connection,60,))
        background_insert[i].start()
    time.sleep(5)

    cluster.check_pbm_status()
    backup=cluster.make_backup("logical")

    Cluster.log("Check chunks distribution after the backup")
    client=pymongo.MongoClient(cluster.connection)
    for chunks in client['config']['chunks'].find():
        print(chunks)

    expected_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Expected count documents for the backup: " + str(expected_docs_count))

    Cluster.log("Wait for background threads are finished")
    for i in range(threads):
        background_insert[i].join()
    Cluster.log("Background threads are finished")

    time.sleep(5)
    Cluster.log("Check chunks distribution before the restore")
    for chunks in client['config']['chunks'].find():
        print(chunks)
    total_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Total count documents: " + str(total_docs_count))

    cluster.make_restore(backup,check_pbm_status=True)
    try:
        actual_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        assert actual_docs_count <= expected_docs_count
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Mongos failure - pymongo assertion error: " + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
