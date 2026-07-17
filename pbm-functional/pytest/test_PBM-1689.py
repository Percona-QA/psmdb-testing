import json
import time

import boto3
import pymongo
import pytest
import testinfra
import yaml

from cluster import Cluster

documents = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

KMS_KEY_ID = "alias/test-pbm-1689"
KMS_REGION = "eu-central-1"

def _aws_credentials():
    with open("conf/pbm/aws.yaml") as f:
        creds = yaml.safe_load(f)["storage"]["s3"]["credentials"]
    return creds["access-key-id"], creds["secret-access-key"]

def kms_client():
    access_key, secret_key = _aws_credentials()
    return boto3.client(
        "kms",
        region_name=KMS_REGION,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

def sts_client():
    access_key, secret_key = _aws_credentials()
    return boto3.client(
        "sts",
        region_name=KMS_REGION,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

def _deny_decrypt(kms, key_id, principal_arn):
    """Add an explicit Deny statement for kms:Decrypt.
    Returns the original policy JSON string so it can be restored afterward.
    """
    original_policy = kms.get_key_policy(KeyId=key_id, PolicyName="default")["Policy"]
    policy = json.loads(original_policy)
    policy["Statement"].append({
        "Sid": "PBM1689DenyDecrypt",
        "Effect": "Deny",
        "Principal": {"AWS": principal_arn},
        "Action": "kms:Decrypt",
        "Resource": "*",
    })
    kms.put_key_policy(KeyId=key_id, PolicyName="default", Policy=json.dumps(policy))
    return original_policy

def _restore_key_policy(kms, key_id, original_policy):
    kms.put_key_policy(KeyId=key_id, PolicyName="default", Policy=original_policy)

@pytest.fixture(scope="function")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="function")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy(cleanup_backups=True)
        cluster.create()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.jenkins
@pytest.mark.timeout(300, func_only=True)
def test_restore_does_not_hang_on_kms_access_denied_PBM_367(start_cluster, cluster):
    """Verify restore and backup does not hang if kms: decrypt access is removed from KMS key policy"""
    cluster.setup_pbm(file="/etc/aws.conf")
    result = cluster.exec_pbm_cli(
        "config --set storage.s3.serverSideEncryption.sseAlgorithm=aws:kms "
        f"--set storage.s3.serverSideEncryption.kmsKeyID={KMS_KEY_ID} --out json -w"
    )
    assert result.rc == 0, result.stdout + result.stderr

    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("logical")

    host = testinfra.get_host("docker://" + cluster.pbm_cli)
    kms = kms_client()
    caller_arn = sts_client().get_caller_identity()["Arn"]

    key_id = kms.describe_key(KeyId=KMS_KEY_ID)["KeyMetadata"]["KeyId"]

    # Testing restore
    original_policy = None
    try:
        # Needed to reset the key back to default
        original_policy = _deny_decrypt(kms, key_id, caller_arn)

        host.run(f"pbm restore -y {backup}")

        # Checking PBM's own status doesn't need S3/KMS access, so decrypt can stay
        # denied for the whole poll -- this is the actual PBM-1689 scenario.
        running = None
        timeout = time.time() + 120
        while time.time() < timeout:
            running = cluster.get_status()["running"]
            if not running:
                break
            time.sleep(5)

        assert not running, "PBM never released the restore lock after 120 seconds."
    finally:
        # Reset Key Policy
        if original_policy is not None:
            _restore_key_policy(kms, key_id, original_policy)

    # Testing backup
    original_policy = None
    try:
        # Needed to reset the key back to default
        original_policy = _deny_decrypt(kms, key_id, caller_arn)

        host.run("pbm backup --out=json")

        running = None
        timeout = time.time() + 120
        while time.time() < timeout:
            running = cluster.get_status()["running"]
            if not running:
                break
            time.sleep(5)

        assert not running, "PBM never released the backup lock after 120 seconds."
    finally:
        if original_policy is not None:
            _restore_key_policy(kms, key_id, original_policy)
