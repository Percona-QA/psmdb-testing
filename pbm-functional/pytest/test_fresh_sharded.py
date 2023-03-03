import pytest
import pymongo
import testinfra
import time
import mongohelper
import pbmhelper
import docker

pytest_plugins = ["docker_compose"]

nodes = ["rs101", "rs102", "rs103", "rs201", "rs202", "rs203", "rscfg01", "rscfg02", "rscfg03"]
configsvr = { "rscfg": [ "rscfg01", "rscfg02", "rscfg03" ]}
sh01 = { "rs1": [ "rs101", "rs102", "rs103" ]}
sh02 = { "rs2": [ "rs201", "rs202", "rs203" ]}
newnodes = ["newrs101", "newrs102", "newrs103", "newrs201", "newrs202", "newrs203", "newrscfg01", "newrscfg02", "newrscfg03"]
newconfigsvr = { "rscfg": [ "newrscfg01", "newrscfg02", "newrscfg03" ]}
newsh01 = { "rs1": [ "newrs101", "newrs102", "newrs103" ]}
newsh02 = { "rs2": [ "newrs201", "newrs202", "newrs203" ]}
connection="mongodb://root:root@mongos:27017/"
newconnection="mongodb://root:root@newmongos:27017/"
documents=[{"a": 1}, {"b": 2}]


@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    time.sleep(5)
    for cluster in [configsvr, sh01, sh02, newconfigsvr, newsh01, newsh02]:
        mongohelper.prepare_rs_parallel([cluster])
    for cluster in [sh01, sh02, newsh01, newsh02]:
        mongohelper.setup_authorization_parallel([cluster])
    mongohelper.setup_authorization("mongos")
    mongohelper.setup_authorization("newmongos")
    pymongo.MongoClient(connection).admin.command("addShard", "rs2/rs201:27017,rs202:27017,rs203:27017")
    pymongo.MongoClient(connection).admin.command("addShard", "rs1/rs101:27017,rs102:27017,rs103:27017")
    pymongo.MongoClient(newconnection).admin.command("addShard", "rs2/newrs201:27017,newrs202:27017,newrs203:27017")
    pymongo.MongoClient(newconnection).admin.command("addShard", "rs1/newrs101:27017,newrs102:27017,newrs103:27017")
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.setup_pbm(nodes[0])
    pbmhelper.setup_pbm(newnodes[0])
    pymongo.MongoClient(connection).admin.command("enableSharding", "test")
    pymongo.MongoClient(connection).admin.command("shardCollection", "test.test", key={"_id": "hashed"})

def test_logical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("rscfg01","logical")
    docker.from_env().containers.get("mongos").kill()
    for node in nodes:
        docker.from_env().containers.get(node).kill()

    pbmhelper.make_resync("newrscfg01")
    pymongo.MongoClient(newconnection).admin.command("balancerStop")
    docker.from_env().containers.get("newmongos").stop()
    pbmhelper.make_restore("newrscfg01",backup)
    docker.from_env().containers.get("newmongos").start()
    time.sleep(5)
    pymongo.MongoClient(newconnection).admin.command("balancerStart")
    assert pymongo.MongoClient(newconnection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newconnection)["test"].command("collstats", "test").get("sharded", False)
    docker.from_env().containers.get("newmongos").kill()
    for node in newnodes:
        docker.from_env().containers.get(node).kill()

def test_physical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("rscfg01","physical")
    docker.from_env().containers.get("mongos").kill()
    for node in nodes:
        docker.from_env().containers.get(node).kill()
    pbmhelper.make_resync("newrscfg01")
    pymongo.MongoClient(newconnection).admin.command("balancerStop")
    docker.from_env().containers.get("newmongos").stop()
    pbmhelper.make_restore("newrscfg01",backup)
    for node in newnodes:
        docker.from_env().containers.get(node).restart()
    for cluster in [newconfigsvr, newsh01, newsh02]:
        mongohelper.wait_for_primary_parallel([cluster],"mongodb://root:root@127.0.0.1:27017/")
    pbmhelper.make_resync("newrscfg01")
    docker.from_env().containers.get("newmongos").start()
    time.sleep(5)
    pymongo.MongoClient(newconnection).admin.command("balancerStart")
    assert pymongo.MongoClient(newconnection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newconnection)["test"].command("collstats", "test").get("sharded", False)
    docker.from_env().containers.get("newmongos").kill()
    for node in newnodes:
        docker.from_env().containers.get(node).kill()

def test_incremental(start_cluster):
    pbmhelper.make_backup("rscfg01","incremental --base")
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("rscfg01","incremental")
    docker.from_env().containers.get("mongos").kill()
    for node in nodes:
        docker.from_env().containers.get(node).kill()
    pbmhelper.make_resync("newrscfg01")
    pymongo.MongoClient(newconnection).admin.command("balancerStop")
    docker.from_env().containers.get("newmongos").stop()
    pbmhelper.make_restore("newrscfg01",backup)
    for node in newnodes:
        docker.from_env().containers.get(node).restart()
    for cluster in [newconfigsvr, newsh01, newsh02]:
        mongohelper.wait_for_primary_parallel([cluster],"mongodb://root:root@127.0.0.1:27017/")
    pbmhelper.make_resync("newrscfg01")
    docker.from_env().containers.get("newmongos").start()
    time.sleep(5)
    pymongo.MongoClient(newconnection).admin.command("balancerStart")
    assert pymongo.MongoClient(newconnection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newconnection)["test"].command("collstats", "test").get("sharded", False)
    docker.from_env().containers.get("newmongos").kill()
    for node in newnodes:
        docker.from_env().containers.get(node).kill()
