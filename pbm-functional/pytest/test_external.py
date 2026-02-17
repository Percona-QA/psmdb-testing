import pytest
import pymongo
import time
import os
import docker

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
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test3", key={"_id": "hashed"})
        yield

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.parametrize('exit', [True, False])
@pytest.mark.timeout(600,func_only=True)
def test_external_meta_PBM_T236(start_cluster,cluster,exit):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.external_backup_start()
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.external_backup_copy(backup)
    cluster.external_backup_finish(backup)
    time.sleep(10)
    restore=cluster.external_restore_start(exit=exit)
    cluster.external_restore_copy(backup)
    cluster.external_restore_finish(restore,exit=exit)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_external_nometa_PBM_T237(start_cluster,cluster):
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.external_backup_start()
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.external_backup_copy(backup)
    cluster.external_backup_finish(backup)
    time.sleep(5)
    os.system("find /backups/ -name pbm.rsmeta.* | xargs rm -f")
    restore=cluster.external_restore_start()
    cluster.external_restore_copy(backup)
    cluster.external_restore_finish(restore)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
