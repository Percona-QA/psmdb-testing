import pytest
import pymongo
import time
import os

import boto3
from botocore.config import Config
from datetime import datetime
from cluster import Cluster


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio1234",
        aws_secret_access_key="minio1234",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
        region_name="us-east-1",
    )

@pytest.fixture(scope="package")
def config():
    return {
        "_id": "rs1",
        "members": [{"host": "rs101"}],
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
    backup = cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"doc": i})
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr(pitr)
    time.sleep(10)
    cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)
    assert (
        pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        == 10
    )
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300, func_only=True)
def test_logical_PBM_T280(start_cluster, cluster):
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"doc": i})
    cluster.disable_pitr()
    time.sleep(10)
    cluster.delete_backup(backup)

    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket="bcp", Prefix="pbme2etest/")
        for obj in page.get("Contents", [])
    ]
    leftover = [k for k in keys if backup in k]
    assert not leftover, f"Leftover artifacts found on storage after delete for {backup}: {leftover}"
    Cluster.log("Storage confirmed clean after backup delete")

    cluster.destroy()

    cluster.create()
    cluster.setup_pbm()
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"doc": i})
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr(pitr)
    time.sleep(10)
    cluster.make_restore(backup, check_pbm_status=True)
    assert (
        pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
        == 10
    )
    Cluster.log("Finished successfully")
