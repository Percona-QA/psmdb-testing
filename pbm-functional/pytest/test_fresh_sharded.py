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

srcconnection="mongodb://root:root@srcmongos:27017/"
dstconnection="mongodb://root:root@dstmongos:27017/"
documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def srccluster():
    return [{ "rscfg": [ "srcrscfg01", "srcrscfg02", "srcrscfg03" ]},{ "rs1": [ "srcrs101", "srcrs102", "srcrs103" ]},{ "rs2": [ "srcrs201", "srcrs202", "srcrs203" ]}]

@pytest.fixture(scope="package")
def dstcluster():
    return [{ "rscfg": [ "dstrscfg01", "dstrscfg02", "dstrscfg03" ]},{ "rs1": [ "dstrs101", "dstrs102", "dstrs103" ]},{ "rs2": [ "dstrs201", "dstrs202", "dstrs203" ]}]

@pytest.fixture(scope="package")
def srcmongos():
    return "srcmongos"

@pytest.fixture(scope="package")
def dstmongos():
    return "dstmongos"

@pytest.fixture(scope="package")
def srcnodes(srccluster):
    n=[]
    for rs in srccluster:
        rsname = list(rs.keys())[0]
        for node in rs[rsname]:
            n.append(node)
    return n

@pytest.fixture(scope="package")
def dstnodes(dstcluster):
    n=[]
    for rs in dstcluster:
        rsname = list(rs.keys())[0]
        for node in rs[rsname]:
            n.append(node)
    return n

@pytest.fixture(scope="function")
def start_cluster(srcmongos,srccluster,srcnodes,dstmongos,dstcluster,dstnodes):
    mongohelper.destroy_sharded(srcmongos,srccluster)
    mongohelper.destroy_sharded(dstmongos,dstcluster)
    connection=mongohelper.create_sharded(srcmongos,srccluster)
    newconnection=mongohelper.create_sharded(dstmongos,dstcluster)
    pbmhelper.restart_pbm_agents(srcnodes + dstnodes)
    pbmhelper.setup_pbm("srcrscfg01")
    pbmhelper.setup_pbm("dstrscfg01")
    client=pymongo.MongoClient(srcconnection)
    client.admin.command("enableSharding", "test")
    client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})

    yield True

    mongohelper.destroy_sharded(srcmongos,srccluster)
    mongohelper.destroy_sharded(dstmongos,dstcluster)


@pytest.mark.timeout(600,func_only=True)
def test_logical(start_cluster,docker_client,srccluster,dstcluster,srcnodes,dstnodes,srcmongos,dstmongos):
    pymongo.MongoClient(srcconnection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("srcrscfg01","logical")
    mongohelper.destroy_sharded(srcmongos,srccluster)
    pbmhelper.make_resync("dstrscfg01")
    pymongo.MongoClient(dstconnection).admin.command("balancerStop")
    docker_client.containers.get(dstmongos).stop()
    pbmhelper.make_restore("dstrscfg01",backup)
    docker_client.containers.get(dstmongos).start()
    time.sleep(5)
    pymongo.MongoClient(dstconnection).admin.command("balancerStart")
    assert pymongo.MongoClient(dstconnection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(dstconnection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(600,func_only=True)
def test_physical(start_cluster,docker_client,srccluster,dstcluster,srcnodes,dstnodes,srcmongos,dstmongos):
    pymongo.MongoClient(srcconnection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("srcrscfg01","physical")
    mongohelper.destroy_sharded(srcmongos,srccluster)
    pbmhelper.make_resync("dstrscfg01")
    pymongo.MongoClient(dstconnection).admin.command("balancerStop")
    docker_client.containers.get(dstmongos).stop()
    pbmhelper.make_restore("dstrscfg01",backup)
    for node in dstnodes:
        docker_client.containers.get(node).restart()
    time.sleep(5)
    mongohelper.wait_for_primary_parallel(dstcluster,"mongodb://root:root@127.0.0.1:27017/")
    pbmhelper.make_resync("dstrscfg01")
    docker_client.containers.get(dstmongos).start()
    time.sleep(5)
    pymongo.MongoClient(dstconnection).admin.command("balancerStart")
    assert pymongo.MongoClient(dstconnection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(dstconnection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

@pytest.mark.timeout(600,func_only=True)
def test_incremental(start_cluster,docker_client,srccluster,dstcluster,srcnodes,dstnodes,srcmongos,dstmongos):
    pbmhelper.make_backup("srcrscfg01","incremental --base")
    pymongo.MongoClient(srcconnection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup("srcrscfg01","incremental")
    mongohelper.destroy_sharded(srcmongos,srccluster)
    pbmhelper.make_resync("dstrscfg01")
    pymongo.MongoClient(dstconnection).admin.command("balancerStop")
    docker_client.containers.get(dstmongos).stop()
    pbmhelper.make_restore("dstrscfg01",backup)
    for node in dstnodes:
        docker_client.containers.get(node).restart()
    time.sleep(5)
    mongohelper.wait_for_primary_parallel(dstcluster,"mongodb://root:root@127.0.0.1:27017/")
    pbmhelper.make_resync("dstrscfg01")
    docker_client.containers.get(dstmongos).start()
    time.sleep(5)
    pymongo.MongoClient(dstconnection).admin.command("balancerStart")
    assert pymongo.MongoClient(dstconnection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(dstconnection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")
