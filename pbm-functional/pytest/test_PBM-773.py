import pytest
import pymongo
import bson
import time
import os
import docker

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
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups "
                                    "--set backup.compression=none --out json --wait")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical_PBM_T221(start_cluster,cluster):
    cluster.check_pbm_status()
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
            collection.insert_one({"i": 9}, session=session)
            collection.insert_one({"j": 10}, session=session)
            collection.insert_one({"k": 11}, session=session)
            collection.insert_one({"l": 12}, session=session)
            session.commit_transaction()
    time.sleep(10)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(10)

    cluster.disable_pitr()
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents) + 8

    folder="/backups/pbmPitr/rs1/" + datetime.utcnow().strftime("%Y%m%d") + "/"
    for entry in os.scandir(folder):
        file = entry.path
    with open(file, "rb") as f:
        data= f.read()
        docs = bson.decode_all(data)
        for doc in docs:
            if "commitTransaction" in doc["o"]:
                if doc["o"]["commitTransaction"] == 1:
                    Cluster.log("Oplog entry with commitTransaction: \n" + str(doc))
                    index = docs.index(doc)
                    Cluster.log("Index: " + str(index))

    Cluster.log("Modifying oplog backup for shard rs1")
    Cluster.log("Removing oplog entry with commitTransaction and all entries after it")
    del docs[index:]
    with open(file, "wb") as f:
        f.truncate()
    with open(file, "ab") as f:
        for doc in docs:
            f.write(bson.encode(doc))
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents) + 8
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")

