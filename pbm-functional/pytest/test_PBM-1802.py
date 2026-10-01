import json
import os
import time

import boto3
import pymongo
import pytest
import testinfra
from botocore.config import Config
from bson.binary import Binary
from cluster import Cluster

# defs.StaleFrameSec - PBM treats an operation whose heartbeat is older than this as abandoned
STALE_FRAME_SEC = 30
TERMINAL_STATUSES = {"done", "error", "canceled"}
BACKUP_ARGS = {
    "logical": "--type=logical",
    "physical": "--type=physical",
    "incremental": "--type=incremental --base",
    "incremental-chain": "--type=incremental"}

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="package")
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

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio1234",
        aws_secret_access_key="minio1234",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
        region_name="us-east-1")

def storage_keys(backup_name):
    paginator = s3_client().get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket="bcp", Prefix="pbme2etest/")
        for obj in page.get("Contents", [])
        if backup_name in obj["Key"]]

def backup_meta(cluster, backup_name):
    client = pymongo.MongoClient(cluster.connection)
    try:
        return client["admin"]["pbmBackups"].find_one({"name": backup_name})
    finally:
        client.close()

def find_snapshot(cluster, backup_name):
    snapshots = cluster.get_status().get("backups", {}).get("snapshot") or []
    return next((s for s in snapshots if s["name"] == backup_name), None)

def fill_data(cluster, docs=50000):
    """Insert an incompressible payload, so a backup is slow enough to be interrupted"""
    client = pymongo.MongoClient(cluster.connection)
    try:
        for start in range(0, docs, 5000):
            client["test"]["data"].insert_many(
                [{"x": i, "pad": Binary(os.urandom(1024))} for i in range(start, min(start + 5000, docs))]
            )
    finally:
        client.close()

def abandon_backup(cluster, backup_args):
    """Start a backup and kill every pbm-agent while it's running"""
    result = cluster.exec_pbm_cli(f"backup {backup_args} --out=json")
    assert result.rc == 0, f"Failed to start backup: {result.stdout} {result.stderr}"
    backup_name = json.loads(result.stdout)["name"]
    timeout = time.time() + 60
    while (backup_meta(cluster, backup_name) or {}).get("status") != "running":
        assert time.time() < timeout, f"Backup {backup_name} was never observed running"
        time.sleep(0.1)
    for host in cluster.pbm_hosts:
        testinfra.get_host("docker://" + host).check_output("kill -9 $(pgrep pbm-agent)")
    Cluster.log(f"Killed pbm-agent while backup {backup_name} was running")
    meta = backup_meta(cluster, backup_name)
    assert meta["status"] not in TERMINAL_STATUSES, (
        f"Backup {backup_name} reached {meta['status']} before the agent was killed, "
        "it completed too fast to be abandoned")
    return backup_name

def wait_until_stuck(cluster, backup_name, timeout=4 * STALE_FRAME_SEC):
    """Wait until PBM reports the abandoned backup as stuck"""
    Cluster.log(f"Waiting for the heartbeat of {backup_name} to go stale")
    client = pymongo.MongoClient(cluster.connection)
    try:
        end = time.time() + timeout
        earliest = time.time() + STALE_FRAME_SEC - 5
        while True:
            client["test"]["tick"].insert_one({"ts": time.time()})
            if time.time() > earliest:
                snapshot = find_snapshot(cluster, backup_name)
                assert snapshot is not None, f"Backup {backup_name} is not listed in pbm status"
                if snapshot["status"] == "error" and "stuck" in snapshot.get("error", "").lower():
                    return
                assert time.time() < end, f"Abandoned backup is not reported as stuck: {snapshot}"
            time.sleep(1)
    finally:
        client.close()

def assert_abandoned_backup_is_deletable(cluster, backup_name, base=None):
    """Assert that abandoned backup can be deleted once its heartbeat is stale
    A single increment is never deletable on its own, it's removed together with
    base of its chain, so base names what gets deleted for abandoned increment
    """
    refusal = "entire chain must be removed" if base else "backup is in progress"
    fresh = cluster.exec_pbm_cli(f"delete-backup {backup_name} --dry-run")
    assert fresh.rc != 0, (
        f"delete-backup accepted {backup_name} while its heartbeat was still fresh: {fresh.stdout} {fresh.stderr}")
    assert refusal in (fresh.stdout + fresh.stderr).lower(), (
        f"Unexpected error for a backup with a fresh heartbeat: {fresh.stdout} {fresh.stderr}")
    if base:
        live = cluster.exec_pbm_cli(f"delete-backup -y {base}")
        assert live.rc != 0, (
            f"delete-backup accepted the chain while its increment was live: {live.stdout} {live.stderr}")
        assert "another operation in progress" in (live.stdout + live.stderr).lower(), (
            f"Unexpected error for a chain with a live increment: {live.stdout} {live.stderr}")
    wait_until_stuck(cluster, backup_name)
    meta = backup_meta(cluster, backup_name)
    assert meta["status"] not in TERMINAL_STATUSES, (
        f"Stored metadata of {backup_name} is already terminal ({meta['status']}), "
        "the stuck-backup deletion is not being exercised")
    stale = cluster.exec_pbm_cli(f"delete-backup {backup_name} --dry-run")
    if base:
        assert stale.rc != 0, f"delete-backup accepted a single increment: {stale.stdout} {stale.stderr}"
        assert refusal in (stale.stdout + stale.stderr).lower(), (
            f"Unexpected error for a single increment: {stale.stdout} {stale.stderr}")
    else:
        assert stale.rc == 0, (
            f"delete-backup refused the abandoned backup {backup_name}: {stale.stdout} {stale.stderr}")
    cluster.restart_pbm_agents()
    cluster.wait_pbm_status()
    Cluster.log(f"Stored status of {backup_name} after the restart: {backup_meta(cluster, backup_name)['status']}")
    delete = cluster.exec_pbm_cli(f"delete-backup -y {base or backup_name}")
    assert delete.rc == 0, f"Failed to delete the abandoned backup: {delete.stdout} {delete.stderr}"
    for name in [base, backup_name] if base else [backup_name]:
        timeout = time.time() + 120
        while find_snapshot(cluster, name) is not None:
            assert time.time() < timeout, f"Backup {name} is still listed in pbm status after the delete"
            time.sleep(1)
        assert backup_meta(cluster, name) is None, f"Metadata of {name} is still in the database"
        leftover = storage_keys(name)
        assert not leftover, f"Leftover artifacts of {name} on storage: {leftover}"

@pytest.mark.timeout(900, func_only=True)
@pytest.mark.parametrize("backup_type", ["logical", "physical", "incremental", "incremental-chain"])
def test_delete_abandoned_backup_PBM_T374(start_cluster, cluster, backup_type):
    """
    Verify that a backup abandoned by a crashed agent can be deleted once its 
    heartbeat is stale, while backup with a fresh heartbeat is still protected
    """
    fill_data(cluster)
    # incremental-chain case abandons a regular increment, which 
    # needs completed base underneath it and new data to copy
    base = None
    if backup_type == "incremental-chain":
        base = cluster.make_backup("incremental --base")
        fill_data(cluster)
    backup_name = abandon_backup(cluster, BACKUP_ARGS[backup_type])
    assert_abandoned_backup_is_deletable(cluster, backup_name, base=base)