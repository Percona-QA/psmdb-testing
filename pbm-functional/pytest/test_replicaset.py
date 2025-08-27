import pytest
import pymongo
import time
import os
import docker

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
    backup_full=cluster.make_backup("logical")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup_full,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == len(documents)
    Cluster.log("Finished successfully")


@pytest.mark.timeout(300, func_only=True)
def test_logical_selective_PBM_T274(start_cluster, cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    for i in range(10):
        client["test1"]["test_coll11"].insert_one({"key": i, "data": i})
        client["test2"]["test_coll21"].insert_one({"key": i, "data": i})
        client["test2"]["test_coll22"].insert_one({"key": i, "data": i})
    client["test1"]["test_coll11"].create_index(["key"], name="test_coll11_index_old")
    client["test2"]["test_coll21"].create_index(["key"], name="test_coll21_index_old")
    backup_full = cluster.make_backup("logical")
    backup_partial = cluster.make_backup("logical --ns=test1.test_coll11,test2.*")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    client.drop_database("test1")
    for i in range(10):
        client["test1"]["test_coll11"].insert_one({"key": i + 10, "data": i + 10})
    client["test1"]["test_coll11"].create_index("data", name="test_coll11_index_new")
    client["test2"]["test_coll22"].create_index("data", name="test_coll22_index_new")
    time.sleep(10)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    pitr = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr(pitr)
    time.sleep(10)
    client.drop_database("test1")
    client.drop_database("test2")
    backup_partial = " --base-snapshot=" + backup_partial + pitr
    backup_full = (
        " --base-snapshot=" + backup_full + pitr + " --ns=test1.test_coll11,test2.*"
    )
    cluster.make_restore(backup_partial, check_pbm_status=True)
    assert client["test1"]["test_coll11"].count_documents({}) == 10
    assert client["test2"]["test_coll21"].count_documents({}) == 10
    assert client["test2"]["test_coll22"].count_documents({}) == 10
    for i in range(10):
        assert client["test1"]["test_coll11"].find_one({"key": i + 10, "data": i + 10})
        assert client["test2"]["test_coll21"].find_one({"key": i, "data": i})
        assert client["test2"]["test_coll22"].find_one({"key": i, "data": i})
    assert (
        "test_coll11_index_old"
        not in client["test1"]["test_coll11"].index_information()
    )
    assert "test_coll11_index_new" in client["test1"]["test_coll11"].index_information()
    assert "test_coll21_index_old" in client["test2"]["test_coll21"].index_information()
    assert "test_coll22_index_new" in client["test2"]["test_coll22"].index_information()
    client.drop_database("test1")
    client.drop_database("test2")
    cluster.make_restore(backup_full, check_pbm_status=True)
    assert client["test1"]["test_coll11"].count_documents({}) == 10
    assert client["test2"]["test_coll21"].count_documents({}) == 10
    assert client["test2"]["test_coll22"].count_documents({}) == 10
    for i in range(10):
        assert client["test1"]["test_coll11"].find_one({"key": i + 10, "data": i + 10})
        assert client["test2"]["test_coll21"].find_one({"key": i, "data": i})
        assert client["test2"]["test_coll22"].find_one({"key": i, "data": i})
    assert (
        "test_coll11_index_old"
        not in client["test1"]["test_coll11"].index_information()
    )
    assert "test_coll11_index_new" in client["test1"]["test_coll11"].index_information()
    assert "test_coll21_index_old" in client["test2"]["test_coll21"].index_information()
    assert "test_coll22_index_new" in client["test2"]["test_coll22"].index_information()
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
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(10)
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
    time.sleep(10)
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == 20
    assert pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({}) == 10
    Cluster.log("Finished successfully")
