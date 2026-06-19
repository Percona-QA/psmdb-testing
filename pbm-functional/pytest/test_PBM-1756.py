import json
import time

import boto3
import pymongo
import pytest
from botocore.config import Config

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

@pytest.fixture(scope="function")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}

@pytest.fixture(scope="function")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy(cleanup_backups=True)
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600, func_only=True)
def test_cancel_backup_PBM_T(start_cluster, cluster):
    """
    Verify that cancelling a backup mid-operation marks it as cancelled and removes all artifacts from storage.
    """
    client = pymongo.MongoClient(cluster.connection)
    col = client["test"]["data"]
    for start in range(0, 20000, 1000):
        col.insert_many([{"x": i, "pad": "x" * 500} for i in range(start, start + 1000)])

    result = cluster.exec_pbm_cli("backup --out=json")
    assert result.rc == 0, f"Failed to start backup: {result.stdout} {result.stderr}"
    backup_name = json.loads(result.stdout)["name"]

    timeout = time.time() + 60
    while True:
        status = cluster.get_status()
        if status.get("running", {}).get("type") == "backup":
            break
        assert time.time() < timeout, "Timed out waiting for backup to start running"
        time.sleep(1)

    Cluster.log("Cancelling backup")
    cancel = cluster.exec_pbm_cli("cancel-backup")
    assert cancel.rc == 0, f"Failed to cancel backup: {cancel.stdout} {cancel.stderr}"

    timeout = time.time() + 60
    while True:
        status = cluster.get_status()
        snapshots = status.get("backups", {}).get("snapshot", [])
        matching = [s for s in snapshots if s["name"] == backup_name]
        if matching and matching[0]["status"] == "canceled":
            Cluster.log("Backup cancelled successfully")
            break
        assert time.time() < timeout, "Timed out waiting for backup to show canceled status"
        time.sleep(2)

    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket="bcp", Prefix="pbme2etest/")
        for obj in page.get("Contents", [])
    ]
    leftover = [k for k in keys if backup_name in k]
    assert not leftover, f"Leftover artifacts found on storage"

@pytest.mark.timeout(300, func_only=True)
def test_cannot_delete_during_backup_PBM_T(start_cluster, cluster):
    """
    Verify that attempting to delete a backup while one is in progress returns the appropriate error.
    """
    client = pymongo.MongoClient(cluster.connection)
    col = client["test"]["data"]
    for start in range(0, 20000, 1000):
        col.insert_many([{"x": i, "pad": "x" * 500} for i in range(start, start + 1000)])

    result = cluster.exec_pbm_cli("backup --out=json")
    assert result.rc == 0, f"Failed to start backup: {result.stdout} {result.stderr}"
    backup_name = json.loads(result.stdout)["name"]
    Cluster.log(f"Started backup: {backup_name}")

    timeout = time.time() + 60
    while True:
        status = cluster.get_status()
        if status.get("running", {}).get("type") == "backup":
            break
        assert time.time() < timeout, "Timed out waiting for backup to start running"
        time.sleep(1)

    delete = cluster.exec_pbm_cli(f"delete-backup -y {backup_name}")
    Cluster.log(f"Delete attempt stdout: {delete.stdout} stderr: {delete.stderr}")
    assert delete.rc != 0, "Expected delete-backup to fail while backup is in progress"
    assert "another operation in progress" in (delete.stdout + delete.stderr).lower(), \
        f"Expected 'another operation in progress' in output, got: {delete.stdout} {delete.stderr}"

    timeout = time.time() + 120
    while True:
        status = cluster.get_status()
        snapshots = status.get("backups", {}).get("snapshot", [])
        matching = [s for s in snapshots if s["name"] == backup_name]
        if matching and matching[0]["status"] == "done":
            Cluster.log("Backup completed successfully after failed delete attempt")
            break
        assert time.time() < timeout, "Timed out waiting for backup to complete"
        time.sleep(2)
