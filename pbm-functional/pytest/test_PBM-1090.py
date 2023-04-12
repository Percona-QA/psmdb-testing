import pytest
import pymongo
import testinfra
import time
import json
import bson
import random
import threading
import os

from cluster import Cluster
from bson import ObjectId

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]}

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
        yield True

    finally:
        print()
        Cluster.log("PBM logs:\n" + cluster.exec_pbm_cli('logs -t 0 -sD').stdout)
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_scenario_1(start_cluster,cluster):
    cluster.check_pbm_status()

    client = pymongo.MongoClient(cluster.connection)
    db = client.test
    collection = db.test
    Cluster.log("Create collection, unique index and insert data")
    collection.insert_one({"a": 1, "b": 1, "c": ObjectId("5d37ace6bb70d20017b2e12f"), "d": 2})
    collection.insert_one({"a": 1, "b": 1, "d": 1})
    collection.insert_one({"a": 1, "b": 1})
    collection.insert_one({"a": 1, "b": 1, "c": ObjectId("5d37ace6bb70d20017b2e12f")})
    collection.create_index([("a",1),("b",1),("c",1),("d",1)], name='test_index', unique = True, background=True, partialFilterExpression={"c": {"$type":"objectId"}})
    res = pymongo.MongoClient(cluster.connection)["test"]["test"].find({})
    Cluster.log('Collection:')
    for r in res:
        Cluster.log(r)

    def upsert_1():
        while upsert:
            query = {"a": 1}
            update = {"$set": {"a": "1", "b": 1, "c":ObjectId("5d37ace6bb70d20017b2e12f"), "d":random.randint(0,1)}}
            pymongo.MongoClient(cluster.connection)['test']['test'].delete_one(query)
            try:
                doc = pymongo.MongoClient(cluster.connection)['test']['test'].find_one_and_update(query,update,upsert=True,return_document=pymongo.collection.ReturnDocument.AFTER)
                Cluster.log(doc)
            except pymongo.errors.DuplicateKeyError:
                pass

    def upsert_2():
        while upsert:
            query = {"b": 1}
            update = {"$set": {"a": "1", "b": 1, "c":ObjectId("5d37ace6bb70d20017b2e12f"), "d":random.randint(0,1)}}
            pymongo.MongoClient(cluster.connection)['test']['test'].delete_one(query)
            try:
                doc = pymongo.MongoClient(cluster.connection)['test']['test'].find_one_and_update(query,update,upsert=True,return_document=pymongo.collection.ReturnDocument.AFTER)
                Cluster.log(doc)
            except pymongo.errors.DuplicateKeyError:
                pass

    upsert=True
    t1 = threading.Thread(target=upsert_1)
    t2 = threading.Thread(target=upsert_2)
    t1.start()
    t2.start()
    start = cluster.exec_pbm_cli('backup --out json').stdout
    name = json.loads(start)['name']
    Cluster.log(name)
    timeout = time.time() + 600
    progress = True
    txn = True
    while progress:
        status = cluster.get_status()
        Cluster.log("Current operation: " + str(status['running']))
        if status['backups']['snapshot']:
            for snapshot in status['backups']['snapshot']:
                if snapshot['name'] == name:
                    if snapshot['status'] == 'done':
                        Cluster.log("Backup found: " + str(snapshot))
                        progress = False
                        break
        if time.time() > timeout:
            assert False, "Backup timeout exceeded"
        time.sleep(0.1)
    upsert=False
    t2.join()
    t1.join()
    res = pymongo.MongoClient(cluster.connection)["test"]["test"].find({})
    Cluster.log('Collection:')
    for r in res:
        Cluster.log(r)

    try:
        cluster.make_restore(name,check_pbm_status=True)
    except AssertionError:
        file = '/backups/' + name + '/rs1/local.oplog.rs.bson'
        with open(file, "rb") as f:
            data= f.read()
            docs = bson.decode_all(data)
            for doc in docs:
                print(doc)
        assert False

    res = pymongo.MongoClient(cluster.connection)["test"]["test"].find({})
    Cluster.log('Collection:')
    for r in res:
        Cluster.log(r)

    Cluster.log("Finished successfully\n")
