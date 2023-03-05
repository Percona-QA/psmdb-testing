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

connection="mongodb://root:root@mongos:27017/"
documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def cluster():
    return [{ "rscfg": [ "rscfg01", "rscfg02", "rscfg03" ]},{ "rs1": [ "rs101", "rs102", "rs103" ]},{ "rs2": [ "rs201", "rs202", "rs203" ]}]

@pytest.fixture(scope="package")
def mongos():
    return "mongos"

@pytest.fixture(scope="package")
def nodes(cluster):
    n=[]
    for rs in cluster:
        rsname = list(rs.keys())[0]
        for node in rs[rsname]:
            n.append(node)
    return n

@pytest.fixture(scope="function")
def start_cluster(mongos,cluster,nodes):
    mongohelper.destroy_sharded(mongos,cluster)

    connection=mongohelper.create_sharded(mongos,cluster)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.setup_pbm("rscfg01")
    client=pymongo.MongoClient(connection)
    client.admin.command("enableSharding", "test")
    client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})

    yield True

    mongohelper.destroy_sharded(mongos,cluster)

@pytest.mark.timeout(300,func_only=True)
def test_logical(start_cluster,docker_client,nodes):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("rscfg01","logical")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker_client.containers.get("mongos").stop()
    pbmhelper.make_restore("rscfg01",backup)
    docker_client.containers.get("mongos").start()
    time.sleep(5)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_physical(start_cluster,docker_client,cluster,nodes):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("rscfg01","physical")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker_client.containers.get("mongos").stop()
    pbmhelper.make_restore("rscfg01",backup)
    for node in nodes:
        docker_client.containers.get(node).restart()
    mongohelper.wait_for_primary_parallel(cluster,"mongodb://root:root@127.0.0.1:27017/")
    pbmhelper.make_resync("rscfg01")
    docker_client.containers.get("mongos").start()
    time.sleep(5)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_incremental(start_cluster,docker_client,cluster,nodes):
    pbmhelper.make_backup("rscfg01","incremental --base")
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("rscfg01","incremental")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker_client.containers.get("mongos").stop()
    pbmhelper.make_restore("rscfg01",backup)
    for node in nodes:
        docker_client.containers.get(node).restart()
    mongohelper.wait_for_primary_parallel(cluster,"mongodb://root:root@127.0.0.1:27017/")
    pbmhelper.make_resync("rscfg01")
    docker_client.containers.get("mongos").start()
    time.sleep(5)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_PBM_773(start_cluster,docker_client,nodes):
    os.chmod("/backups",0o777)
    n = testinfra.get_host("docker://rscfg01")
    n.check_output("pbm config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none")
    time.sleep(10)
    pbmhelper.make_backup("rscfg01","logical")
    pbmhelper.enable_pitr("rscfg01")

    client = pymongo.MongoClient(connection)
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

    pbmhelper.disable_pitr("rscfg01")
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker_client.containers.get("mongos").stop()
    pbmhelper.make_restore("rscfg01",backup)
    docker_client.containers.get("mongos").start()
    time.sleep(5)
    mongohelper.wait_for_primary("mongos",connection)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    results = pymongo.MongoClient(connection)["test"]["test"].find({})
    for result in results:
        print(result)
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents) + 4

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
    pymongo.MongoClient(connection).admin.command("balancerStop")
    docker_client.containers.get("mongos").stop()
    pbmhelper.make_restore("rscfg01",backup)
    docker_client.containers.get("mongos").start()
    time.sleep(5)
    mongohelper.wait_for_primary("mongos",connection)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
    results = pymongo.MongoClient(connection)["test"]["test"].find({})
    for result in results:
        print(result)
    print("\nFinished successfully\n")
