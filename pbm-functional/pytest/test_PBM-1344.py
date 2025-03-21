import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading

from datetime import datetime
from cluster import Cluster

documents = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="package")
def config():
    return {
        "_id": "rs1",
        "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}],
    }


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(600, func_only=True)
def test_physical_PBM_T279(start_cluster, cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    backup = cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"doc": i})
    cluster.disable_pitr()
    time.sleep(10)
    cluster.delete_backup(backup)
    cluster.destroy()

    cluster.create()
    cluster.setup_pbm()
    time.sleep(10)
    cluster.check_pbm_status()
    backup = cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"doc": i})
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr()
    time.sleep(10)
    cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)
    assert (
        pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        == 10
    )
    Cluster.log("Finished successfully")


@pytest.mark.timeout(300, func_only=True)
def test_logical_PBM_T280(start_cluster, cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"doc": i})
    cluster.disable_pitr()
    time.sleep(10)
    cluster.delete_backup(backup)
    cluster.destroy()

    cluster.create()
    cluster.setup_pbm()
    time.sleep(10)
    cluster.check_pbm_status()
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"doc": i})
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr()
    time.sleep(10)
    cluster.make_restore(backup, check_pbm_status=True)
    assert (
        pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        == 10
    )
    Cluster.log("Finished successfully")
