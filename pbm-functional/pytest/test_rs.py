import pytest
import pymongo
import testinfra
import time
import mongohelper
import pbmhelper

pytest_plugins = ["docker_compose"]

nodes=["rs101","rs102","rs103"]
rsname="rs"
connection="mongodb://root:root@rs101:27017,rs102:27017,rs103:27017/?replicaSet=rs"
documents=[{"a": 1}, {"b": 2}]

@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    time.sleep(5)
    mongohelper.prepare_rs(rsname,nodes)
    mongohelper.setup_authorization(nodes[0])
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.setup_pbm(nodes[0])

def test_logical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"logical")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pbmhelper.make_restore(nodes[0],backup)
    result=pymongo.MongoClient(connection)["test"]["test"].find()
    for document in result:
        assert document in documents

def test_physical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"physical")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
    result=pymongo.MongoClient(connection)["test"]["test"].find()
    for document in result:
        assert document in documents

def test_incremental(start_cluster):
    pbmhelper.make_backup(nodes[0],"incremental --base")
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    result=pymongo.MongoClient(connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
    result=pymongo.MongoClient(connection)["test"]["test"].find()
    for document in result:
        assert document in documents
