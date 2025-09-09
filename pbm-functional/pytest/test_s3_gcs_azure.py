import os
from datetime import timedelta

import docker
import pymongo
import pytest
import base64
import time
from bson.binary import Binary

from cluster import Cluster


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.jenkins
@pytest.mark.parametrize(
    ("provider", "encryption_type"),[
        pytest.param("aws", "no-encryption"),
        pytest.param("aws", "sse-c"),
        pytest.param("aws", "sse-kms"),
        pytest.param("aws", "sse-s3"),
        pytest.param("gcs", "no-encryption"),
        pytest.param("gcs_hmac", "no-encryption"),
        pytest.param("azure", "no-encryption")])
@pytest.mark.parametrize("backup_type", ["logical", "physical", "incremental"])
@pytest.mark.timeout(800, func_only=True)
def test_general_PBM_T300(start_cluster, cluster, provider, encryption_type, backup_type):
    """
    Test PBM with various cloud providers and encryption types.

    - Configure PBM with the given provider and encryption
    - Insert data and perform backup(s) with PITR
    - Restore from PITR and verify data integrity
    - Clean up and check PBM logs for errors
    """
    cloud_configs = {
        "aws": "/etc/aws.conf",
        "gcs": "/etc/gcs.conf",
        "gcs_hmac": "/etc/gcs_hmac.conf",
        "azure": "/etc/azure.conf"}
    cluster.setup_pbm(file=cloud_configs[provider])
    client = pymongo.MongoClient(cluster.connection)
    mongod_version = client.server_info()["version"]
    major_ver = "".join(mongod_version.split(".")[:2])
    unique_prefix = f"{encryption_type}/{major_ver}-{backup_type}"
    if provider == "aws":
        if encryption_type == 'sse-c':
            key_bytes = bytes([i % 256 for i in range(32)])
            sse_customer_key = base64.b64encode(key_bytes).decode('utf-8')
            result = cluster.exec_pbm_cli(f'config --set storage.s3.prefix={unique_prefix} '
            f'--set storage.s3.serverSideEncryption.sseCustomerAlgorithm=AES256 '
            f'--set storage.s3.serverSideEncryption.sseCustomerKey={sse_customer_key} '
            f'--out json -w')
            assert result.rc == 0
        elif encryption_type == 'sse-kms':
            kms_key_id = os.environ.get("KMS_ID")
            assert kms_key_id, "KMS_ID environment variable is not set"
            result = cluster.exec_pbm_cli(f'config --set storage.s3.prefix={unique_prefix} '
            f'--set storage.s3.serverSideEncryption.sseAlgorithm=aws:kms '
            f'--set storage.s3.serverSideEncryption.kmsKeyID={kms_key_id} --out json -w')
            assert result.rc == 0
        elif encryption_type == 'sse-s3':
            result = cluster.exec_pbm_cli(f'config --set storage.s3.prefix={unique_prefix} '
            '--set storage.s3.serverSideEncryption.sseAlgorithm=AES256 --out json -w')
            assert result.rc == 0
    if encryption_type == "no-encryption":
        if provider == "aws":
            result = cluster.exec_pbm_cli(f'config --set storage.s3.prefix={unique_prefix} --out json -w')
        elif provider in ["gcs", "gcs_hmac"]:
            result = cluster.exec_pbm_cli(f'config --set storage.gcs.prefix={unique_prefix} --out json -w')
        elif provider == "azure":
            result = cluster.exec_pbm_cli(f'config --set storage.azure.prefix={unique_prefix} --out json -w')
        assert result.rc == 0
    cluster.check_pbm_status()
    result = cluster.exec_pbm_cli("config")
    Cluster.log("Current PBM config:\n" + result.stdout)
    # Add 20MB of data to verify multipart upload
    total_docs = (20 * 1024) // 10
    for i in range(0, total_docs, 1000):
        batch = [
            {"_id": i + j, "payload": Binary(os.urandom(10 * 1024))}
            for j in range(min(1000, total_docs - i))]
        client["test"]["bigdata"].insert_many(batch)
    Cluster.log(f"Inserted {total_docs} documents")
    if backup_type == "incremental":
        cluster.make_backup(f"{backup_type} --base")
    else:
        cluster.make_backup(f"{backup_type}")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    # Perform second logical backup to check that PITR is able to copy oplog from backup
    if backup_type == "logical":
        cluster.make_backup(f"{backup_type}")
    for i in range(10):
        client["test"]["test_coll11"].insert_one({"key": i, "data": i})
        client["test"]["test_coll21"].insert_one({"key": i, "data": i})
    # As PITR target use TS of last oplog for test_coll21 + 2 seconds
    last_op = (client.local["oplog.rs"].find({"ns": "test.test_coll21", "op": "i"})
               .sort("$natural", -1).limit(1)[0])
    ts = last_op["ts"].as_datetime() + timedelta(seconds=2)
    pitr = ts.strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Time for PITR is: " + pitr)
    backup="--time=" + pitr
    cluster.disable_pitr(pitr)
    client.drop_database("test")

    if backup_type == "logical":
        cluster.make_restore(backup,timeout=500,check_pbm_status=True)
    else:
        cluster.make_restore(backup,timeout=500,restart_cluster=True,check_pbm_status=True)
    assert client["test"]["test_coll11"].count_documents({}) == 10
    assert client["test"]["test_coll21"].count_documents({}) == 10
    assert client["test"]["bigdata"].count_documents({}) == total_docs
    for i in range(total_docs):
        assert client["test"]["bigdata"].find_one({"_id": i})
    for i in range(10):
        assert client["test"]["test_coll11"].find_one({"key": i, "data": i})
        assert client["test"]["test_coll21"].find_one({"key": i, "data": i})
    def pbm_exec_and_wait(cluster, cmd, *, delay=2):
        result = cluster.exec_pbm_cli(cmd)
        assert result.rc == 0
        timeout = time.time() + 120
        while time.time() < timeout:
            if not cluster.get_status().get("running", False):
                return
            time.sleep(delay)
        raise TimeoutError(f"PBM command {cmd!r} is still running after timeout")
    pbm_exec_and_wait(cluster, "delete-pitr --all --force --yes --wait")
    pbm_exec_and_wait(cluster, f"delete-backup --older-than=0d -t {backup_type} --force --yes")
    logs=cluster.exec_pbm_cli("logs -sD -t0")
    ignored_errors = ["no documents in result","send pbm heartbeat","resync"]
    error_lines = [
        line for line in logs.stdout.splitlines()
        if " E " in line and not any(ignore in line for ignore in ignored_errors)]
    if error_lines:
        error_summary = "\n".join(error_lines)
        raise AssertionError(f"Errors found in PBM logs\n{error_summary}")
    Cluster.log("Finished successfully")
