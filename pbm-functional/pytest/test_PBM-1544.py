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
    return { "_id": "rs1", "members": [{"host":"rs101"}]}

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
def test_success_backup_status(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    cluster.make_backup("physical")
    assert cluster.get_status()["backups"]["snapshot"][0]['printStatus'] == "success"

@pytest.mark.timeout(300,func_only=True)
def test_ongoing_backup_status(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    cluster.make_backup("physical")
    assert cluster.get_status()["backups"]["snapshot"][0]['printStatus'] == "success"