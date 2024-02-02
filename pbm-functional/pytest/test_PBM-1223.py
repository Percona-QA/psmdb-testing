import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading
import concurrent.futures

from datetime import datetime
from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]}
                      ]}

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
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none --out json")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.testcase(test_case_key="T249", test_step_key=1)
@pytest.mark.timeout(300,func_only=True)
def test_disabled(start_cluster,cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    db = client.test
    collection = db.test
    with client.start_session() as session:
        with session.start_transaction():
            Cluster.log("Transaction started\n")
            collection.insert_one({"e": 5}, session=session)
            collection.insert_one({"f": 6}, session=session)
            collection.insert_one({"g": 7}, session=session)
            collection.insert_one({"h": 8}, session=session)
            collection.insert_one({"i": 9}, session=session)
            background_backup=concurrent.futures.ThreadPoolExecutor().submit(cluster.make_backup, 'logical')
            time.sleep(1)
            collection.insert_one({"j": 10}, session=session)
            collection.insert_one({"k": 11}, session=session)
            collection.insert_one({"l": 12}, session=session)
            session.commit_transaction()
            Cluster.log("Transaction commited\n")
    backup=background_backup.result()
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 8
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 8
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")
