import json

import boto3
import pymongo
import pytest
from botocore.config import Config
from bson import decode_all

from cluster import Cluster

def generate_data(client, count, offset=0, batch_size=1000):
    """Insert documents into database"""
    for start in range(offset, offset + count, batch_size):
        end = min(start + batch_size, offset + count)
        client["test"]["data"].insert_many([{"x": i, "pad": "x" * 500} for i in range(start, end)])

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio1234",
        aws_secret_access_key="minio1234",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
        region_name="us-east-1",
    )

def get_backup_storage_size(backup_name):
    """Returns the total size in bytes of all objects stored in minio for the given backup name."""
    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    total = sum(
        obj["Size"]
        for page in paginator.paginate(Bucket="bcp", Prefix=f"pbme2etest/{backup_name}/")
        for obj in page.get("Contents", [])
    )
    return total

def get_uncompressed_size_from_filelist(backup_name):
    """Calculate size_uncompressed by reading filelist.pbm from minio for each RS."""
    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket="bcp", Prefix=f"pbme2etest/{backup_name}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("filelist.pbm"):
                continue
            data = s3.get_object(Bucket="bcp", Key=key)["Body"].read()
            docs = decode_all(data)
            total += sum(d.get("fileSize", 0) for d in docs if d.get("stgSize", 0) != 0)
            total += obj["Size"]
    return total

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}

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

@pytest.mark.timeout(300, func_only=True)
def test_compression_size_uncompressed_PBM_T318(start_cluster, cluster):
    """Verify size_uncompressed_h matches size_h if backup.compression=none is set for a non-based incremental backup."""
    result = cluster.exec_pbm_cli("config --set backup.compression=none --wait")
    assert result.rc == 0, f"Failed to set backup.compression=none: rc={result.rc}, stdout={result.stdout}, stderr={result.stderr}"

    client = pymongo.MongoClient(cluster.connection)
    generate_data(client, count=100000)

    cluster.make_backup("incremental --base")

    generate_data(client, count=50000, offset=100000)

    incr_backup = cluster.make_backup("incremental")

    result = cluster.exec_pbm_cli(f"describe-backup {incr_backup} --out=json")
    assert result.rc == 0, f"describe-backup failed: {result.stderr}"
    incr_desc = json.loads(result.stdout)
    Cluster.log(f"Increment backup - size_h: {incr_desc['size_h']}, size_uncompressed_h: {incr_desc['size_uncompressed_h']}")

    assert incr_desc["size"] == incr_desc["size_uncompressed"], f"Increment size: ({incr_desc['size_h']}) should equal size_uncompressed: ({incr_desc['size_uncompressed_h']})."

    incr_storage_total = get_backup_storage_size(incr_backup)
    assert incr_desc["size_uncompressed"] == incr_storage_total, (
        f"Increment size_uncompressed ({incr_desc['size_uncompressed_h']}) does not match "
        f"actual uncompressed storage total ({incr_storage_total} bytes). "
    )

@pytest.mark.timeout(300, func_only=True)
def test_incremental_size_uncompressed_with_compression_PBM_T319(start_cluster, cluster):
    """Verify size_uncompressed_h matches uncompressed file size of non based incremental backup."""
    cluster.check_pbm_status()

    client = pymongo.MongoClient(cluster.connection)
    generate_data(client, count=100000)

    cluster.make_backup("incremental --base")

    generate_data(client, count=50000, offset=100000)

    incr_backup = cluster.make_backup("incremental")

    result = cluster.exec_pbm_cli(f"describe-backup {incr_backup} --out=json")
    assert result.rc == 0, f"describe-backup failed: {result.stderr}"
    incr_desc = json.loads(result.stdout)
    Cluster.log(f"Increment backup - size_h: {incr_desc['size_h']}, size_uncompressed_h: {incr_desc['size_uncompressed_h']}")

    assert incr_desc["size"] != incr_desc[
        "size_uncompressed"], f"Increment size: ({incr_desc['size_h']}) should not equal size_uncompressed: ({incr_desc['size_uncompressed_h']})."

    filelist_uncompressed = get_uncompressed_size_from_filelist(incr_backup)
    assert incr_desc["size_uncompressed"] == filelist_uncompressed, (
        f"Increment size_uncompressed ({incr_desc['size_uncompressed_h']}) does not match "
        f"uncompressed size derived from filelist.pbm ({filelist_uncompressed} bytes)."
    )
