import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker

from datetime import datetime
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"},{"host": "rs102"},{"host": "rs103"},
#                                      {"host": "rs104"},{"host": "rs105"},{"host": "rs106"},{"host": "rs107"}
                                      {"host": "rs104", "hidden": True, "priority": 0, "votes": 0},
                                      {"host": "rs105", "hidden": True, "priority": 0, "votes": 0},
                                      {"host": "rs106", "hidden": True, "priority": 0, "votes": 0},
                                      {"host": "rs107", "hidden": True, "priority": 0, "votes": 0}
                                     ]}

@pytest.fixture(scope="package")
def pbm_mongodb_uri():
    return 'mongodb://pbm:pbmpass@127.0.0.1:27017/?authSource=admin&w=3&readConcernLevel=local'

@pytest.fixture(scope="package")
def cluster(config,pbm_mongodb_uri):
    return Cluster(config, pbm_mongodb_uri=pbm_mongodb_uri)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups "
                                    "--set backup.compression=none --out json --wait")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical_PBM_T254(start_cluster,cluster):
    cluster.check_pbm_status()
    for i in range(100):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"a": i})
    backup = cluster.make_backup("logical")
    backup_status = cluster.exec_pbm_cli("describe-backup " + backup).stdout
    Cluster.log("Backup status: \n" + backup_status)
    cluster.make_resync()

    cluster.check_pbm_status()
    backup_status = cluster.exec_pbm_cli("describe-backup " + backup).stdout
    Cluster.log("Backup status: \n" + backup_status)
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 100
    Cluster.log("Finished successfully")

