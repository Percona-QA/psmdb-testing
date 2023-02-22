import pytest
import pymongo
import testinfra
import time
import mongohelper
import pbmhelper

pytest_plugins = ["docker_compose"]

nodes=["rs101","rs102","rs103"]
newnodes=["newrs101","newrs102","newrs103"]
rsname="rs"
rs = { "rs": [ "rs101", "rs102", "rs103" ]}
newrs = { "rs": [ "newrs101", "newrs102", "newrs103" ]}
connection="mongodb://root:root@rs101:27017,rs102:27017,rs103:27017/?replicaSet=rs"
newconnection="mongodb://root:root@newrs101:27017,newrs102:27017,newrs103:27017/?replicaSet=rs"
documents=[{"a": 1}, {"b": 2}]

@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    time.sleep(5)
    mongohelper.prepare_rs_parallel([rs, newrs])
    mongohelper.setup_authorization_parallel([rs, newrs])
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.setup_pbm(nodes[0])
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.setup_pbm(newnodes[0])

def test_logical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    #perform backup on old cluster
    backup=pbmhelper.make_backup(nodes[0],"logical")
    #resync storage on new cluster
    pbmhelper.make_resync(newnodes[0])
    #perform restore on new cluster
    pbmhelper.make_restore(newnodes[0],backup)
    result=pymongo.MongoClient(newconnection)["test"]["test"].find()
    for document in result:
        print(document)
        assert document in documents

def test_physical(start_cluster):
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"physical")
    pbmhelper.make_resync(newnodes[0])
    pbmhelper.make_restore(newnodes[0],backup)
    mongohelper.restart_mongod(newnodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.make_resync(newnodes[0])
    result=pymongo.MongoClient(newconnection)["test"]["test"].find()
    for document in result:
        assert document in documents

def test_incremental(start_cluster):
    pbmhelper.make_backup(nodes[0],"incremental --base")
    pymongo.MongoClient(connection)["test"]["test"].insert_many(documents)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    pbmhelper.make_resync(newnodes[0])
    pbmhelper.make_restore(newnodes[0],backup)
    mongohelper.restart_mongod(newnodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.make_resync(newnodes[0])
    result=pymongo.MongoClient(newconnection)["test"]["test"].find()
    for document in result:
        assert document in documents
