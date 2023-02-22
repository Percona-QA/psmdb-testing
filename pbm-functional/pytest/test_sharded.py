import pytest
import pymongo
import testinfra
import time
import mongohelper
import pbmhelper

pytest_plugins = ["docker_compose"]

nodes = ["rscfg01", "rscfg02", "rscfg03", "rs101", "rs102", "rs103", "rs201", "rs202", "rs203"]
configsvr = { "rscfg": [ "rscfg01", "rscfg02", "rscfg03" ]}
sh01 = { "rs1": [ "rs101", "rs102", "rs103" ]}
sh02 = { "rs2": [ "rs201", "rs202", "rs203" ]}
connection="mongodb://root:root@mongos:27017/"
documents=[{"a": 1, "shard_key": 1}, {"b": 2, "shard_key": 2}]

@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    time.sleep(5)
    mongohelper.prepare_rs_parallel([configsvr, sh01, sh02])
    mongohelper.setup_authorization_parallel([sh01, sh02])
    time.sleep(5)
    mongohelper.setup_authorization("mongos")
    pymongo.MongoClient(connection).admin.command("addShard", "rs2/rs201:27017,rs202:27017,rs203:27017")
    pymongo.MongoClient(connection).admin.command("addShard", "rs1/rs101:27017,rs102:27017,rs103:27017")
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.setup_pbm(nodes[0])
    pymongo.MongoClient(connection).admin.command("enableSharding", "test")
    pymongo.MongoClient(connection)["test"]["test"].create_index("shard_key")
    pymongo.MongoClient(connection).admin.command("shardCollection", "test.test", key={"shard_key": 1})

def test_logical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"logical")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    pbmhelper.make_restore(nodes[0],backup)
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)

def test_physical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"physical")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)

def test_incremental(start_cluster):
    pbmhelper.make_backup(nodes[0],"incremental --base")
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pymongo.MongoClient(connection).admin.command("balancerStop")
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
    pymongo.MongoClient(connection).admin.command("balancerStart")
    assert pymongo.MongoClient(connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(connection)["test"].command("collstats", "test").get("sharded", False)
