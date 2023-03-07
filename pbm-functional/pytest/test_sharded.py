import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading

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
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_logical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("logical")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_physical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,restart_cluster=True, make_resync=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_incremental(start_cluster,cluster):
    cluster.check_pbm_status()
    cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("incremental")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,restart_cluster=True, make_resync=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_PBM_773(start_cluster,cluster):
    os.chmod("/backups",0o777)
    result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none")
    assert result.rc == 0
    print(result.stdout)
    time.sleep(10)
    cluster.make_backup("logical")
    cluster.enable_pitr()

    client = pymongo.MongoClient(cluster.connection)
    db = client.test
    collection = db.test
    collection.insert_many(documents)
    time.sleep(10)

    with client.start_session() as session:
        with session.start_transaction():
            collection.insert_one({"e": 5}, session=session)
            collection.insert_one({"f": 6}, session=session)
            collection.insert_one({"g": 7}, session=session)
            collection.insert_one({"h": 8}, session=session)
            session.commit_transaction()
    time.sleep(10)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    print("\npitr time is:")
    print(pitr)
    time.sleep(10)

    cluster.disable_pitr()
    cluster.make_restore(backup,check_pbm_status=True)
    results = pymongo.MongoClient(cluster.connection)["test"]["test"].find({})
    for result in results:
        print(result)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents) + 4

    folder="/backups/pbmPitr/rs1/" + datetime.utcnow().strftime("%Y%m%d") + "/"
    for entry in os.scandir(folder):
        file = entry.path
    with open(file, "rb") as f:
        data= f.read()
        docs = bson.decode_all(data)
        print("oplog entry for rs1")
        print(docs)
        for doc in docs:
            if "commitTransaction" in doc["o"]:
                if doc["o"]["commitTransaction"] == 1:
                    print("\noplog entry with commitTransaction")
                    print(doc)
                    index = docs.index(doc)
                    print("\nindex")
                    print(index)

    del docs[index:]
    with open(file, "wb") as f:
        f.truncate()
    with open(file, "ab") as f:
        for doc in docs:
            f.write(bson.encode(doc))
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    results = pymongo.MongoClient(cluster.connection)["test"]["test"].find({})
    for result in results:
        print(result)
    print("\nFinished successfully\n")
