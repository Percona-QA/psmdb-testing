import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import concurrent.futures
import random
import json

from datetime import datetime
from cluster import Cluster
from packaging import version

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]}

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
#        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none --out json")
#        assert result.rc == 0
#        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        client=pymongo.MongoClient(cluster.connection)
        for i in range(10):
            client["test"]["test"].insert_one({"key": i, "data": i})
            client["test"]["inserts"].insert_one({"key": i, "data": i})
            client["test"]["replaces"].insert_one({"key": i, "data": i})
            client["test"]["updates"].insert_one({"key": i, "data": i})
            client["test"]["deletes"].insert_one({"key": i, "data": i})
            client["test"]["indexes"].insert_one({"key": i, "data": i})
        client["test"]["indexes"].create_index(["key"],name="old_index")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.parametrize('collection',['inserts','replaces','updates','deletes','indexes'])
def test_logical_pitr_crud_PBM_T270(start_cluster,cluster,collection):
    cluster.check_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    for i in range(10):
        client = pymongo.MongoClient(cluster.connection)
        client["test"]["inserts"].insert_one({"key": i+10, "data": i+10})
        client["test"]["replaces"].replace_one({"key": i}, {"key": i+10, "data": i+10})
        client["test"]["updates"].update_one({"key": i}, {"$inc": { "data": 10 }})
        client["test"]["deletes"].delete_one({"key": i})
    client["test"]["indexes"].drop_index("old_index")
    client["test"]["indexes"].create_index("data",name="new_index")
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    pitr=" --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(10)
    cluster.disable_pitr()
    time.sleep(5)
    backup_to_fail=pitr + " --ns-from=test." + collection + " --ns-to=test.test"
    result=cluster.exec_pbm_cli("restore" + backup_to_fail + " --wait")
    assert result.rc != 0
    assert "cloning namespace (--ns-to) is already in use" in result.stderr
    backup=pitr + " --ns-from=test." + collection + " --ns-to=restored." + collection
    cluster.make_restore(backup)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["inserts"].count_documents({})==20
    assert client["test"]["replaces"].count_documents({})==10
    assert client["test"]["updates"].count_documents({})==10
    assert client["test"]["deletes"].count_documents({})==0
    if collection=='inserts':
        assert client["restored"]["inserts"].count_documents({})==20
        for i in range(20):
            assert client["restored"]["inserts"].find_one({"key": i, "data": i})
    elif collection=='replaces':
        assert client["restored"]["replaces"].count_documents({})==10
        for i in range(10):
            assert client["restored"]["replaces"].find_one({"key": i+10, "data": i+10})
    elif collection=='updates':
        assert client["restored"]["updates"].count_documents({})==10
        for i in range(10):
            assert client["restored"]["updates"].find_one({"key": i, "data": i+10})
    elif collection=='deletes':
        assert client["restored"]["deletes"].count_documents({})==0
    else:
        assert client["restored"]["indexes"].count_documents({})==10
        assert 'new_index' in client["restored"]["indexes"].index_information()
        assert 'old_index' not in client["restored"]["indexes"].index_information()
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.parametrize('collection',['inserts','replaces','updates','deletes'])
def test_logical_pitr_with_txn_PBM_T271(start_cluster,cluster,collection):
    cluster.check_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    for i in range(10):
        client = pymongo.MongoClient(cluster.connection)
        with client.start_session() as session:
            with session.start_transaction():
                client["test"]["inserts"].insert_one({"key": i+10, "data": i+10}, session=session)
                client["test"]["replaces"].replace_one({"key": i}, {"key": i+10, "data": i+10}, session=session)
                client["test"]["updates"].update_one({"key": i}, {"$inc": { "data": 10 }}, session=session)
                client["test"]["deletes"].delete_one({"key": i}, session=session)
            session.commit_transaction()
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    pitr=" --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(10)
    cluster.disable_pitr()
    time.sleep(5)
    backup_to_fail=pitr + " --ns-from=test." + collection + " --ns-to=test.test"
    result=cluster.exec_pbm_cli("restore" + backup_to_fail + " --wait")
    assert result.rc != 0
    assert "cloning namespace (--ns-to) is already in use" in result.stderr
    backup=pitr + " --ns-from=test." + collection + " --ns-to=restored." + collection
    cluster.make_restore(backup)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["inserts"].count_documents({})==20
    assert client["test"]["replaces"].count_documents({})==10
    assert client["test"]["updates"].count_documents({})==10
    assert client["test"]["deletes"].count_documents({})==0
    if collection=='inserts':
        assert client["restored"]["inserts"].count_documents({})==20
        for i in range(20):
            assert client["restored"]["inserts"].find_one({"key": i, "data": i})
    elif collection=='replaces':
        assert client["restored"]["replaces"].count_documents({})==10
        for i in range(10):
            assert client["restored"]["replaces"].find_one({"key": i+10, "data": i+10})
    elif collection=='updates':
        assert client["restored"]["updates"].count_documents({})==10
        for i in range(10):
            assert client["restored"]["updates"].find_one({"key": i, "data": i+10})
    else:
        assert client["restored"]["deletes"].count_documents({})==0
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_logical_pitr_ddl_PBM_T273(start_cluster,cluster):
    cluster.check_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    client = pymongo.MongoClient(cluster.connection)
    client.drop_database('test')
    for i in range(10):
        client["test"]["indexes"].insert_one({"key": i+10, "data": i+10})
    client["test"]["indexes"].create_index("data",name="new_index")
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    pitr=" --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(10)
    cluster.disable_pitr()
    time.sleep(5)
    backup=pitr + " --ns-from=test.indexes --ns-to=restored.indexes"
    cluster.make_restore(backup)
    client = pymongo.MongoClient(cluster.connection)
    assert client["restored"]["indexes"].count_documents({})==10
    for i in range(10):
        assert client["restored"]["indexes"].find_one({"key": i+10, "data": i+10})
    assert 'new_index' in client["restored"]["indexes"].index_information()
    assert 'old_index' not in client["restored"]["indexes"].index_information()
    Cluster.log("Finished successfully")
