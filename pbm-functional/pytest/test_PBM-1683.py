import json

import boto3
import pymongo
import pytest
from botocore.config import Config

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

def _decode_uvarint(data, pos):
    """Decode a little-endian unsigned varint at pos, return (value, new_pos)."""
    result = 0
    shift = 0
    while True:
        b = data[pos]
        pos += 1
        result |= (b & 0x7f) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7

def _s2_uncompressed_size(data):
    """Return the total uncompressed byte count encoded in an S2 stream's frame headers."""
    CRC_SIZE = 4
    pos = 0
    total = 0
    while pos < len(data):
        chunk_type = data[pos]
        chunk_len = int.from_bytes(data[pos + 1:pos + 4], "little")
        pos += 4
        if chunk_type == 0x00:  # compressed block: CRC + uvarint(uncompressed_len) + payload
            uncompressed_len, _ = _decode_uvarint(data, pos + CRC_SIZE)
            total += uncompressed_len
        elif chunk_type == 0x01:  # uncompressed block: CRC + raw bytes
            total += chunk_len - CRC_SIZE
        # 0xff (stream identifier) and others: skip
        pos += chunk_len
    return total


def get_uncompressed_size_from_s2_files(backup_name):
    """Calculate uncompressed size by parsing S2 frame headers of all data files in the backup."""
    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket="bcp", Prefix=f"pbme2etest/{backup_name}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if ".s2" in key.split("/")[-1]:
                data = s3.get_object(Bucket="bcp", Key=key)["Body"].read()
                total += _s2_uncompressed_size(data)
            elif key.endswith("filelist.pbm"):
                total += obj["Size"]
    return total

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
    """Verify size_uncompressed_h matches size_h if backup.compression=none match in describe-backup"""
    result = cluster.exec_pbm_cli("config --set backup.compression=none --wait")
    assert result.rc == 0, f"Failed to set backup.compression=none: rc={result.rc}, stdout={result.stdout}, stderr={result.stderr}"

    client = pymongo.MongoClient(cluster.connection)
    generate_data(client, count=100000)

    base_backup = cluster.make_backup("incremental --base")

    generate_data(client, count=50000, offset=100000)

    incr_backup = cluster.make_backup("incremental")
    phys_backup = cluster.make_backup("physical")

    result = cluster.exec_pbm_cli(f"describe-backup {base_backup} --out=json")
    assert result.rc == 0, f"describe-backup failed: {result.stderr}"
    base_desc = json.loads(result.stdout)
    Cluster.log(f"Base backup - size_h: {base_desc['size_h']}, size_uncompressed_h: {base_desc['size_uncompressed_h']}")

    result = cluster.exec_pbm_cli(f"describe-backup {incr_backup} --out=json")
    assert result.rc == 0, f"describe-backup failed: {result.stderr}"
    incr_desc = json.loads(result.stdout)
    Cluster.log(f"Increment backup - size_h: {incr_desc['size_h']}, size_uncompressed_h: {incr_desc['size_uncompressed_h']}")

    result = cluster.exec_pbm_cli(f"describe-backup {phys_backup} --out=json")
    assert result.rc == 0, f"describe-backup failed: {result.stderr}"
    phys_desc = json.loads(result.stdout)
    Cluster.log(
        f"Physical backup - size_h: {phys_desc['size_h']}, size_uncompressed_h: {phys_desc['size_uncompressed_h']}")

    assert base_desc["size"] == base_desc["size_uncompressed"], f"Base size: ({base_desc['size_h']}) should equal size_uncompressed: ({base_desc['size_uncompressed_h']})."

    assert incr_desc["size"] == incr_desc["size_uncompressed"], f"Increment size: ({incr_desc['size_h']}) should equal size_uncompressed: ({incr_desc['size_uncompressed_h']})."

    assert phys_desc["size"] == phys_desc["size_uncompressed"], f"Physical size: ({phys_desc['size_h']}) should equal size_uncompressed: ({phys_desc['size_uncompressed_h']})."

    base_storage_total = get_backup_storage_size(base_backup)
    assert base_desc["size_uncompressed"] == base_storage_total, (
        f"Base size_uncompressed ({base_desc['size_uncompressed_h']}) does not match "
        f"actual uncompressed storage total ({base_storage_total} bytes). "
    )

    incr_storage_total = get_backup_storage_size(incr_backup)
    assert incr_desc["size_uncompressed"] == incr_storage_total, (
        f"Increment size_uncompressed ({incr_desc['size_uncompressed_h']}) does not match "
        f"actual uncompressed storage total ({incr_storage_total} bytes). "
    )

    phys_storage_total = get_backup_storage_size(phys_backup)
    assert phys_desc["size_uncompressed"] == phys_storage_total, (
        f"Physical size_uncompressed ({phys_desc['size_uncompressed_h']}) does not match "
        f"actual uncompressed storage total ({phys_storage_total} bytes). "
    )

@pytest.mark.timeout(300, func_only=True)
def test_incremental_size_uncompressed_with_compression_PBM_T319(start_cluster, cluster):
    """Verify size_uncompressed_h matches uncompressed file size in describe-backup"""
    cluster.check_pbm_status()

    client = pymongo.MongoClient(cluster.connection)
    generate_data(client, count=100000)

    base_backup = cluster.make_backup("incremental --base")

    generate_data(client, count=50000, offset=100000)

    incr_backup = cluster.make_backup("incremental")
    phys_backup = cluster.make_backup("physical")

    result = cluster.exec_pbm_cli(f"describe-backup {base_backup} --out=json")
    assert result.rc == 0, f"describe-backup failed: {result.stderr}"
    base_desc = json.loads(result.stdout)
    Cluster.log(f"Base backup - size_h: {base_desc['size_h']}, size_uncompressed_h: {base_desc['size_uncompressed_h']}")

    result = cluster.exec_pbm_cli(f"describe-backup {incr_backup} --out=json")
    assert result.rc == 0, f"describe-backup failed: {result.stderr}"
    incr_desc = json.loads(result.stdout)
    Cluster.log(f"Increment backup - size_h: {incr_desc['size_h']}, size_uncompressed_h: {incr_desc['size_uncompressed_h']}")

    result = cluster.exec_pbm_cli(f"describe-backup {phys_backup} --out=json")
    assert result.rc == 0, f"describe-backup failed: {result.stderr}"
    phys_desc = json.loads(result.stdout)
    Cluster.log(f"Physical backup - size_h: {phys_desc['size_h']}, size_uncompressed_h: {phys_desc['size_uncompressed_h']}")

    assert base_desc["size"] != base_desc[
        "size_uncompressed"], f"Base size: ({base_desc['size_h']}) should not equal size_uncompressed: ({base_desc['size_uncompressed_h']})."

    assert incr_desc["size"] != incr_desc[
        "size_uncompressed"], f"Increment size: ({incr_desc['size_h']}) should not equal size_uncompressed: ({incr_desc['size_uncompressed_h']})."

    assert phys_desc["size"] != phys_desc[
        "size_uncompressed"], f"Physical size: ({phys_desc['size_h']}) should not equal size_uncompressed: ({phys_desc['size_uncompressed_h']})."

    base_s2_uncompressed = get_uncompressed_size_from_s2_files(base_backup)
    assert base_desc["size_uncompressed"] == base_s2_uncompressed, (
        f"Base size_uncompressed ({base_desc['size_uncompressed_h']}) does not match "
        f"uncompressed size derived from S2 files ({base_s2_uncompressed} bytes)."
    )

    incr_s2_uncompressed = get_uncompressed_size_from_s2_files(incr_backup)
    assert incr_desc["size_uncompressed"] == incr_s2_uncompressed, (
        f"Increment size_uncompressed ({incr_desc['size_uncompressed_h']}) does not match "
        f"uncompressed size derived from S2 files ({incr_s2_uncompressed} bytes)."
    )

    phys_s2_uncompressed = get_uncompressed_size_from_s2_files(phys_backup)
    assert phys_desc["size_uncompressed"] == phys_s2_uncompressed, (
        f"Physical size_uncompressed ({phys_desc['size_uncompressed_h']}) does not match "
        f"uncompressed size derived from S2 files ({phys_s2_uncompressed} bytes)."
    )
