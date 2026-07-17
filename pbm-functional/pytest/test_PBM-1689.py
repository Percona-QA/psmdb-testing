import json
import re
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
    original_policy = None
    kms = key_id = None
    try:
        cluster.destroy(cleanup_backups=True)
        cluster.create()

        cluster.setup_pbm(file="/etc/aws.conf")
        result = cluster.exec_pbm_cli(
            "config --set storage.s3.serverSideEncryption.sseAlgorithm=aws:kms "
            f"--set storage.s3.serverSideEncryption.kmsKeyID={KMS_KEY_ID} --out json -w"
        )
        assert result.rc == 0, result.stdout + result.stderr

        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
        backup = cluster.make_backup("logical")

        kms = kms_client()
        caller_arn = sts_client().get_caller_identity()["Arn"]
        key_id = kms.describe_key(KeyId=KMS_KEY_ID)["KeyMetadata"]["KeyId"]
        original_policy = _deny_decrypt(kms, key_id, caller_arn)

        yield backup
    finally:
        # Do not reorder lines, destroy requires access to S3 which restore_key_policy allows
        if original_policy is not None:
            _restore_key_policy(kms, key_id, original_policy)
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.jenkins
@pytest.mark.timeout(300, func_only=True)
def test_restore_does_not_hang_on_kms_access_denied_PBM_367(start_cluster, cluster):
    """Verify restore and backup does not hang if kms: decrypt access is removed from KMS key policy"""
    backup = start_cluster
    host = testinfra.get_host("docker://" + cluster.pbm_cli)

    # Testing restore
    restore_start = host.run(f"pbm restore -y {backup}")
    match = re.search(r"Starting restore (\S+) from", restore_start.stdout)
    assert match, f"Restore was never dispatched.\nSTDOUT: {restore_start.stdout}\nSTDERR: {restore_start.stderr}"
    restore_name = match.group(1)

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

    describe_restore = json.loads(host.run(f"pbm describe-restore {restore_name} --out=json").stdout)
    restore_error = describe_restore.get("error", "")
    if restore_error:
        assert "AccessDenied" in restore_error and "kms:Decrypt" in restore_error, (
            f"Restore failed for an unexpected reason: {restore_error}")

    # Testing backup
    backup_start = json.loads(host.run("pbm backup --out=json").stdout)
    backup_name = backup_start["name"]

    running = None
    timeout = time.time() + 120
    while time.time() < timeout:
        running = cluster.get_status()["running"]
        if not running:
            break
        time.sleep(5)

    assert not running, "PBM never released the backup lock after 120 seconds."

    describe_backup = json.loads(host.run(f"pbm describe-backup {backup_name} --out=json").stdout)
    backup_error = describe_backup.get("error", "")
    if backup_error:
        assert "AccessDenied" in backup_error and "kms:Decrypt" in backup_error, (
            f"Backup failed for an unexpected reason: {backup_error}")
