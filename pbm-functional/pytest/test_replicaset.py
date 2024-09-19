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

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

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
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_many(documents)
    backup_partial=cluster.make_backup("logical --ns=test.test")
    backup_full=cluster.make_backup("logical")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup_partial,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == 0
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup_full,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == len(documents)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_physical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_incremental(start_cluster,cluster):
    cluster.check_pbm_status()
    cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("incremental")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_logical_timeseries_PBM_T224(start_cluster,cluster):
    cluster.check_pbm_status()
    client=pymongo.MongoClient(cluster.connection)
    mongod_version=client.server_info()["version"]
    if version.parse(mongod_version) < version.parse("5.0.0"):
        pytest.skip("Unsupported version for timeseries")
    pymongo.MongoClient(cluster.connection)["test"].create_collection('test1',timeseries={'timeField':'timestamp','metaField': 'data'})
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_one({"timestamp": datetime.now(), "data": i})
        time.sleep(0.1)
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    time.sleep(30)
    #create new timeseries collection
    pymongo.MongoClient(cluster.connection)["test"].create_collection('test2',timeseries={'timeField':'timestamp','metaField': 'data'})
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_one({"timestamp": datetime.now(), "data": i})
        pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_one({"timestamp": datetime.now(), "data": i})
        time.sleep(0.1)
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(30)
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == 20
    assert pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({}) == 10
    Cluster.log("Finished successfully")
