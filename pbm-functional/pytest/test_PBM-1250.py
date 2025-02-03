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
#        if version.parse(mongod_version) >= version.parse("7.0.0"):
#            Cluster.log("Set autoMergerIntervalSecs to 30 sec")
#            result = pymongo.MongoClient('mongodb://root:root@rscfg01:27017?authSource=admin&replicaSet=rscfg').admin.command( { "setParameter": 1, "autoMergerIntervalSecs": 30 } )
#            print(result)
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

@pytest.mark.parametrize('backup_t',['logic','phys'])
@pytest.mark.timeout(600,func_only=True)
def test_load_pitr(start_cluster,cluster,backup_t):
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

    threads = 100
    Cluster.log("Start inserting docs in the background with " + str(threads) + " threads ")
    background_insert = [None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs)
        background_insert[i].start()
    time.sleep(5)

    cluster.check_pbm_status()
    backup_type = 'physical' if backup_t == 'phys' else 'logical'
    cluster.make_backup(backup_type)
    expected_docs_count_base = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Expected count documents for base backup: " + str(expected_docs_count_base))
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    Cluster.log("Check chunks distribution after the backup")
    client=pymongo.MongoClient(cluster.connection)
    for chunks in client['config']['chunks'].find():
        print(chunks)

    time.sleep(30)
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
    cluster.disable_pitr()
    time.sleep(5)
    Cluster.log("Check chunks distribution before the restore")
    for chunks in client['config']['chunks'].find():
        print(chunks)
    total_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    Cluster.log("Total count documents: " + str(total_docs_count))

    restart = True if backup_type == 'physical' else False
    cluster.make_restore(backup,restart_cluster=restart,check_pbm_status=True)

    try:
        actual_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        assert actual_docs_count >= expected_docs_count_pitr
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Pymongo failure\n:" + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")


@pytest.mark.parametrize('restart_after_restore',[True,False])
@pytest.mark.parametrize('restore_to_fresh_cluster',[True,False])
@pytest.mark.timeout(600,func_only=True)
def test_load_base(start_cluster,cluster,restart_after_restore,restore_to_fresh_cluster):
    def insert_docs():
        client=pymongo.MongoClient(cluster.connection)
        timeout = time.time() + 60
        while True:
            id = 'user' + str(random.randint(10**12,10**13-1))
            data = random.randbytes(10*1024)
            client['test']['test'].insert_one({'_id':id,'data': data})
            if time.time() > timeout:
                break

    threads = 100
    Cluster.log("Start inserting docs in the background with " + str(threads) + " threads ")
    background_insert = [None] * threads
    for i in range(threads):
        background_insert[i] = threading.Thread(target=insert_docs)
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
    if restore_to_fresh_cluster:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none --out json")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        cluster.check_pbm_status()
        cluster.make_resync()
    else:
        Cluster.log("Check chunks distribution before the restore")
        for chunks in client['config']['chunks'].find():
            print(chunks)
        total_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        Cluster.log("Total count documents: " + str(total_docs_count))

    cluster.make_restore(backup,restart_cluster=restart_after_restore,check_pbm_status=True)
    try:
        actual_docs_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        assert actual_docs_count <= expected_docs_count
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Pymongo failure\n:" + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
