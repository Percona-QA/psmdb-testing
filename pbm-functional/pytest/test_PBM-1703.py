from time import sleep

import boto3
import pymongo
import pytest
import testinfra
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

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        n = testinfra.get_host("docker://rs101")
        n.check_output("sed -i 's|pbm-agent --mongodb-uri|pbm-agent --log-level=D --mongodb-uri|' /etc/supervisord.d/pbm-agent.ini")
        Cluster.restart_pbm_agent("rs101")
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(120,func_only=True)
def test_PBM_T322(start_cluster,cluster):
    """Verify that PBM does not perform unnecessary requests for .pbm.init.pbmpart.1"""
    cluster.check_pbm_status()
    cluster.exec_pbm_cli("config --set storage.s3.debugLogLevels=Request,Response --wait")

    # To allow enough time for the agentCheck to trigger
    sleep(15)

    n = testinfra.get_host("docker://rs101")
    logs = n.check_output("pbm logs -sD -t0")

    assert ".pbm.init.pbmpart.1" not in logs, "HEAD request for .pbm.init.pbmpart.1 found in PBM logs"

    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket="bcp", Prefix="pbme2etest/.pbm.init")
        for obj in page.get("Contents", [])
    ]
    assert not any(".pbmpart.1" in key for key in keys), \
        "Unexpected .pbm.init.pbmpart.1 file found on storage"


@pytest.mark.timeout(3600, func_only=True)
def test_PBM_T323(start_cluster, cluster):
    """Verify that multipart splitting works correctly for large backup files and that data is fully restored."""
    cluster.check_pbm_status()
    cluster.exec_pbm_cli("config --set backup.compression=none,storage.s3.maxObjSizeGB=1 --wait")

    # Adding enough data to produce a backup file over 2GB (compression disabled)
    client = pymongo.MongoClient(cluster.connection)
    pad = "x" * 10000
    for batch_start in range(0, 230000, 1000):
        client["test"]["data"].insert_many([{"x": batch_start + i, "pad": pad} for i in range(1000)])

    backup = cluster.make_backup("logical")

    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket="bcp", Prefix=f"pbme2etest/{backup}/")
        for obj in page.get("Contents", [])
    ]
    assert any(".pbmpart.1" in key for key in keys), \
        f"Expected .pbmpart.1 file in backup {backup} not found"
    assert any(".pbmpart.2" in key for key in keys), \
        f"Expected .pbmpart.2 file in backup {backup} not found"
    assert not any(".pbmpart.3" in key for key in keys), \
        f"Unexpected .pbmpart.3 file found in backup {backup}"

    cluster.make_restore(backup)
    cluster.check_pbm_status()

    restored_count = client["test"]["data"].count_documents({})
    assert restored_count == 230000, f"Expected 230000 documents after restore, got {restored_count}"
