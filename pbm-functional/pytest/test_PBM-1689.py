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

KMS_KEY_ID = "alias/keith-test-pbm-1689"
KMS_REGION = "us-east-1"
S3_BUCKET = "pbm-keith-test-2"

# Jenkins populates this file with real AWS credentials at build time (see
# conf/pbm/aws.yaml / /etc/aws.conf); boto3 has no other way to pick them up here.
AWS_CONF_FILE = "conf/pbm/aws.yaml"

# Generous bound on how long we let the restore sit "running" before calling it stuck.
# Real-world PBM-1689 hangs were multi-hour; this just needs to comfortably clear PBM's
# own internal ~120s PITR-slicer-stop wait plus retryChunk's backoff (45s worst case).
RESTORE_STUCK_TIMEOUT = 300
RESTORE_POLL_INTERVAL = 5


def _aws_credentials():
    with open(AWS_CONF_FILE) as f:
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
    """Add an explicit Deny statement for kms:Decrypt, scoped to the given principal.

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


# Requires a real AWS account: an S3 bucket configured via /etc/aws.conf (populated at
# Jenkins runtime), the pbm-keith-test-2 bucket, and the keith-test-pbm-1689 KMS key in
# us-east-1.
@pytest.mark.jenkins
@pytest.mark.timeout(3600, func_only=True)
def test_restore_does_not_hang_on_kms_access_denied_PBM_1689(start_cluster, cluster):
    """Testing Doc
    """
    cluster.setup_pbm(file="/etc/aws.conf")
    result = cluster.exec_pbm_cli(
        f"config --set storage.s3.bucket={S3_BUCKET} "
        f"--set storage.s3.region={KMS_REGION} "
        "--set storage.s3.serverSideEncryption.sseAlgorithm=aws:kms "
        f"--set storage.s3.serverSideEncryption.kmsKeyID={KMS_KEY_ID} --out json -w"
    )
    assert result.rc == 0, result.stdout + result.stderr

    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("physical")

    host = testinfra.get_host("docker://" + cluster.pbm_cli)
    kms = kms_client()
    caller_arn = sts_client().get_caller_identity()["Arn"]

    original_policy = None
    try:
        # Revoke access exactly the way the ticket describes: edit the key policy to
        # deny kms:Decrypt for the identity PBM uses, rather than swapping S3
        # credentials or disabling the whole key. It only blocks Decrypt for this one
        # caller, not every operation for every principal.
        original_policy = _deny_decrypt(kms, KMS_KEY_ID, caller_arn)

        start_result = host.run(f"pbm restore -y {backup}")
        match = re.search(r"Starting restore (\S+) from", start_result.stdout)
        assert match, (
            f"Restore was never dispatched.\nSTDOUT: {start_result.stdout}\nSTDERR: {start_result.stderr}"
        )
        restore_name = match.group(1)

        # The real symptom is that PBM holds the restore lock forever, which shows up
        # as "another operation in progress" on any later command. "pbm status" /
        # "describe-restore" can be blind to this when the failure happens early, so we
        # poll by retrying a real command instead.
        deadline = time.time() + RESTORE_STUCK_TIMEOUT
        probe_result = None
        while time.time() < deadline:
            _restore_key_policy(kms, KMS_KEY_ID, original_policy)
            probe_result = host.run("pbm config --set storage.s3.prefix=pbm-1689-kms-probe --wait")
            if probe_result.rc == 0:
                break
            if "another operation in progress" not in (probe_result.stderr or "").lower():
                break
            time.sleep(RESTORE_POLL_INTERVAL)

        pbm_logs = host.run("pbm logs -sD -t0")
        Cluster.log("PBM agent logs after restore attempt:\n" + pbm_logs.stdout + pbm_logs.stderr)

        assert probe_result is not None and probe_result.rc == 0, (
            f"PBM never released the restore lock for {restore_name} after "
            f"{RESTORE_STUCK_TIMEOUT}s -- this is the PBM-1689 hang.\n"
            f"Last error: {probe_result.stderr if probe_result else 'N/A'}"
        )
    finally:
        # Always put the original key policy back, even if an assertion above failed,
        # so teardown (which talks to storage) and the next test run aren't left broken.
        if original_policy is not None:
            _restore_key_policy(kms, KMS_KEY_ID, original_policy)
