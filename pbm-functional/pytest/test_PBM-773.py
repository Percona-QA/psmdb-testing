import pytest
import pymongo
import bson
import time
import os

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
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.setup_pbm("/etc/pbm-fs.conf")
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
    cluster.make_backup("logical")
    cluster.enable_pitr()

    client = pymongo.MongoClient(cluster.connection)
    db = client.test
    collection = db.test
    for i in range(50):
        collection.insert_one({})
    time.sleep(10)

    with client.start_session() as session:
        with session.start_transaction():
            for i in range(50):
                collection.insert_one({}, session=session)
            session.commit_transaction()
    time.sleep(10)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(10)

    cluster.disable_pitr(pitr)
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 100

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
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 100
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")

