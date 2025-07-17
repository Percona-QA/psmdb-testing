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
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]},
                            {"_id": "rs2", "members": [{"host":"rs201"}]},
                            {"_id": "rs3", "members": [{"host":"rs301"}]}
                      ]}

@pytest.fixture(scope="package")
def mongod_extra_args():
    return ' --setParameter=logicalSessionRefreshMillis=10000'

@pytest.fixture(scope="package")
def cluster(config,mongod_extra_args):
    return Cluster(config,  mongod_extra_args=mongod_extra_args)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        cluster.exec_pbm_cli("config --set backup.compression=none --out json")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical_PBM_T301(start_cluster,cluster):
    """
    The test for situation when system.session is created before the backup/restore
    on both source cluster and target cluster
    """
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({})
    time.sleep(20)
    backup=cluster.make_backup("logical")
    cluster.destroy()
    cluster.create()
    cluster.setup_pbm()
    time.sleep(20)
    try:
        cluster.make_restore(backup,check_pbm_status=True)
    except AssertionError:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Restore failed \n\nMongos logs:\n" + mongos_logs
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1
    Cluster.log("Finished successfully")
