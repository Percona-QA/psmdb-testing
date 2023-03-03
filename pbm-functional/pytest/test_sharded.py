import pytest
import pymongo
import bson
import testinfra
import time
import mongohelper
import pbmhelper
import os
import docker

from datetime import datetime

pytest_plugins = ["docker_compose"]

nodes = ["rs101", "rs102", "rs103", "rs201", "rs202", "rs203", "rscfg01", "rscfg02", "rscfg03"]
configsvr = { "rscfg": [ "rscfg01", "rscfg02", "rscfg03" ]}
sh01 = { "rs1": [ "rs101", "rs102", "rs103" ]}
sh02 = { "rs2": [ "rs201", "rs202", "rs203" ]}
connection="mongodb://root:root@mongos:27017/"
documents=[{"a": 1}, {"b": 2}]

@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    time.sleep(5)
    for cluster in [configsvr, sh01, sh02]:
        mongohelper.prepare_rs_parallel([cluster])
    for cluster in [sh01, sh02]:
        mongohelper.setup_authorization_parallel([cluster])
    mongohelper.setup_authorization("mongos")
    client = pymongo.MongoClient(connection)
    client.admin.command("addShard", "rs2/rs201:27017,rs202:27017,rs203:27017")
    client.admin.command("addShard", "rs1/rs101:27017,rs102:27017,rs103:27017")
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.setup_pbm(nodes[0])
    client.admin.command("enableSharding", "test")
    client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})

def test_logical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"logical")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker.from_env().containers.get("mongos").stop()
    pbmhelper.make_restore(nodes[0],backup)
    docker.from_env().containers.get("mongos").start()
    time.sleep(5)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
    docker.from_env().containers.get("mongos").kill()
    for node in nodes:
        docker.from_env().containers.get(node).kill()

def test_physical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"physical")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker.from_env().containers.get("mongos").stop()
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
    docker.from_env().containers.get("mongos").start()
    time.sleep(5)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
    docker.from_env().containers.get("mongos").kill()
    for node in nodes:
        docker.from_env().containers.get(node).kill()

def test_incremental(start_cluster):
    pbmhelper.make_backup(nodes[0],"incremental --base")
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker.from_env().containers.get("mongos").stop()
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
    docker.from_env().containers.get("mongos").start()
    time.sleep(5)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
    docker.from_env().containers.get("mongos").kill()
    for node in nodes:
        docker.from_env().containers.get(node).kill()

def test_PBM_773(start_cluster):
    os.chmod("/backups",0o777)
    n = testinfra.get_host("docker://" + nodes[0])
    n.check_output("pbm config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none")
    time.sleep(10)
    pbmhelper.make_backup(nodes[0],"logical")
    pbmhelper.enable_pitr(nodes[0])

    client = pymongo.MongoClient(connection)
    db = client.test
    collection = db.test
    collection.insert_many(documents)
    time.sleep(10)

    with client.start_session() as session:
        with session.start_transaction():
            collection.insert_one({"c": 3}, session=session)
            collection.insert_one({"d": 4}, session=session)
            session.commit_transaction()
    time.sleep(10)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    print("\npitr time is:")
    print(pitr)
    time.sleep(10)

    pbmhelper.disable_pitr(nodes[0])
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker.from_env().containers.get("mongos").stop()
    pbmhelper.make_restore(nodes[0],backup)
    docker.from_env().containers.get("mongos").start()
    pymongo.MongoClient(connection).admin.command("balancerStart")
    results = pymongo.MongoClient(connection)["test"]["test"].find({})
    for result in results:
        print(result)
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents) + 2

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
                    index = docs.index(doc)

    del docs[index:]
    with open(file, "wb") as f:
        f.truncate()
    with open(file, "ab") as f:
        for doc in docs:
            f.write(bson.encode(doc))
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker.from_env().containers.get("mongos").stop()
    pbmhelper.make_restore(nodes[0],backup)
    docker.from_env().containers.get("mongos").start()
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
    results = pymongo.MongoClient(connection)["test"]["test"].find({})
    for result in results:
        print(result)
    docker.from_env().containers.get("mongos").kill()
    for node in nodes:
        docker.from_env().containers.get(node).kill()
