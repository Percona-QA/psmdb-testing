import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import concurrent.futures
import random
import json

from datetime import datetime
from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none --out json")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test1", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.testcase(test_case_key="T249", test_step_key=1)
@pytest.mark.timeout(300,func_only=True)
def test_logical(start_cluster,cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    db = client.test
    collection = db.test
    with client.start_session() as session:
        with session.start_transaction():
            Cluster.log("Transaction started\n")
            collection.insert_one({"e": 5}, session=session)
            collection.insert_one({"f": 6}, session=session)
            collection.insert_one({"g": 7}, session=session)
            collection.insert_one({"h": 8}, session=session)
            collection.insert_one({"i": 9}, session=session)
            background_backup=concurrent.futures.ThreadPoolExecutor().submit(cluster.make_backup, 'logical')
            time.sleep(1)
            collection.insert_one({"j": 10}, session=session)
            collection.insert_one({"k": 11}, session=session)
            collection.insert_one({"l": 12}, session=session)
            session.commit_transaction()
            Cluster.log("Transaction commited\n")
    backup=background_backup.result()
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 8
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 8
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")

@pytest.mark.timeout(3600,func_only=True)
@pytest.mark.parametrize('backup_t',['logic','phys'])
def test_load(start_cluster,cluster,backup_t):
    backup_type = 'logical'
    if backup_t == "phys":
        backup_type = 'physical'
    # run transactions, returns array of tuples each includes oplog timestamp, oplog increment, resulted documents count
    def background_transaction(db,collection):
        Cluster.log("Starting background insert to " + collection)
        j = 0
        result = []
        while upsert:
            data = random.randbytes(2 * 1024 * 1024)
            client = pymongo.MongoClient(cluster.connection)
            with client.start_session() as session:
                try:
                    with session.start_transaction():
                        for i in range(20):
                            client[db][collection].insert_one({str(i): data }, session=session)
                        timeout = random.uniform(0.4,0.6)
                        time.sleep(timeout)
                        session.commit_transaction()
                        j = j + 20
                        Cluster.log(collection + ": (" + str(session.cluster_time['clusterTime'].time) + " " + str(session.cluster_time['clusterTime'].inc)  + ") " + str(j))
                        result.append((session.cluster_time['clusterTime'].time,session.cluster_time['clusterTime'].inc,j))
                except pymongo.errors.PyMongoError as e:
                    Cluster.log(e)
                    continue
                finally:
                    client.close()
        Cluster.log("Stopping background insert to " + collection)
        return result
    pymongo.MongoClient('mongodb://root:root@rs101:27017').admin.command( { "setParameter": 1, "wiredTigerEngineRuntimeConfig": "cache_size=1G"} )
    pymongo.MongoClient('mongodb://root:root@rs201:27017').admin.command( { "setParameter": 1, "wiredTigerEngineRuntimeConfig": "cache_size=1G"} )
    cluster.check_pbm_status()
    upsert=True
    background_transaction1 = concurrent.futures.ThreadPoolExecutor().submit(background_transaction, 'test', 'test1')
    background_transaction2 = concurrent.futures.ThreadPoolExecutor().submit(background_transaction, 'test', 'test2')

    time.sleep(60)
    try:
        backup=cluster.make_backup(backup_type)
    except AssertionError as e:
        upsert=False
        background_transaction1.result()
        background_transaction2.result()
        assert False, e

    upsert=False
    upsert1_result = background_transaction1.result()
    upsert2_result = background_transaction2.result()
    Cluster.log("test1 documents count: " + str(pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({})))
    Cluster.log("test2 documents count: " + str(pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({})))

    # backup_meta=json.loads(cluster.exec_pbm_cli("describe-backup " + backup + " --out=json").stdout)
    # since pbm describe-backup doesn't return exact oplog timestamp let's check metadata on the storage
    backup_meta=json.loads(testinfra.get_host("docker://rscfg01").check_output('cat /backups/' + backup + '.pbm.json'))
    Cluster.log(json.dumps(backup_meta, indent=4))
    last_write_ts = (backup_meta["last_write_ts"]["T"],backup_meta["last_write_ts"]["I"])

    # let's find the real count of documents inserted till last_write_ts
    # we have the array of tuples containing oplog timestamp and resulted documents count like
    # [(1709134884, 23, 20), (1709134886, 5, 40), (1709134887, 39, 60), (1709134889, 6, 80), (1709134891, 5, 100), (1709134955, 4, 120)]
    # the second argument is last_write_ts like 1709134954.11
    # the result should be (1709134891, 5, 100)
    # result should be t[1] from the resulted tuple
    def find_inserted(array_tuples,timestamp):
        print(array_tuples)
        print(timestamp)
        filtered_first = [t for t in array_tuples if ( t[0] < timestamp[0] ) or ( t[0] == timestamp[0] and t[1] <= timestamp[1] ) ]
        max_first = max(t[0] for t in filtered_first)
        filtered_second = [t for t in filtered_first if t[0] == max_first]
        result = max(filtered_second, key=lambda t: t[1])
        print(result)
        return result[2]

    inserted_test1 = find_inserted(upsert1_result,last_write_ts)
    inserted_test2 = find_inserted(upsert2_result,last_write_ts)
    Cluster.log("test1 inserted count: " + str(inserted_test1))
    Cluster.log("test2 inserted count: " + str(inserted_test2))

    if backup_type == "logical":
        restart = False
    else:
        restart = True
    cluster.make_restore(backup,restart_cluster=restart,check_pbm_status=True,timeout=600)

    count_test1 = pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({})
    count_test2 = pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({})
    Cluster.log("test1 documents count: " + str(count_test1))
    Cluster.log("test2 documents count: " + str(count_test2))

    assert inserted_test1 == count_test1 or inserted_test1 + 20 == count_test1
    assert inserted_test2 == count_test2 or inserted_test2 + 20 == count_test2
    Cluster.log("Finished successfully\n")
