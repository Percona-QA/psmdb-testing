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

@pytest.mark.parametrize('encryption_type',['sse-c','sse-kms','sse-s3'])
#@pytest.mark.parametrize('encryption_type',['sse-s3'])
@pytest.mark.timeout(6000, func_only=True)
def test_logical_PBM_T216(start_cluster, cluster, encryption_type):
    if encryption_type == 'sse-c':
        result = cluster.exec_pbm_cli("config --set storage.s3.bucket=bcp1 --set storage.s3.serverSideEncryption.sseAlgorithm=AES256 --set storage.s3.serverSideEncryption.sseCustomerKey=\"0y4YWYJA8mNZF6dum+BxUfUUiefYpTimYHwyXawgLkM=\" --out json")
        assert result.rc == 0
    elif encryption_type == 'sse-kms':
        result = cluster.exec_pbm_cli("config --set storage.s3.bucket=bcp2 --set storage.s3.serverSideEncryption.sseAlgorithm=aws:kms storage.s3.serverSideEncryption.kmsKeyID=minio-key --out json")
        assert result.rc == 0
    elif encryption_type == 'sse-s3':
        result = cluster.exec_pbm_cli("config --set storage.s3.bucket=bcp3 --set storage.s3.serverSideEncryption.sseAlgorithm=AES256 --out json")
        assert result.rc == 0
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    for i in range(10):
        client["test1"]["test_coll11"].insert_one({"key": i, "data": i})
        client["test2"]["test_coll21"].insert_one({"key": i, "data": i})
    time.sleep(5)
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    cluster.make_backup("logical")
    backup = cluster.make_backup("physical")
    # add check for errors in logs for pitr
    cluster.disable_pitr()
    time.sleep(5)
    client.drop_database("test1")
    client.drop_database("test2")
    result = cluster.exec_pbm_cli(f"delete-pitr --all -y")
    assert result.rc == 0
    result = cluster.exec_pbm_cli(f"delete-backup --older-than=0d -t logical -y")
    assert result.rc == 0

    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert client["test1"]["test_coll11"].count_documents({}) == 10
    assert client["test2"]["test_coll21"].count_documents({}) == 10
    for i in range(10):
        assert client["test1"]["test_coll11"].find_one({"key": i, "data": i})
        assert client["test2"]["test_coll21"].find_one({"key": i, "data": i})
    Cluster.log("Finished successfully")
