import json
import pytest
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

def check_backup_exists(cluster, backup):
    result = cluster.exec_pbm_cli("status -s backups --out json")

    status = json.loads(result.stdout)

    snapshots = status.get("backups", {}).get("snapshot", [])
    matching = [s for s in snapshots if s.get("name") == backup]
    if matching:
        return True
    else:
        return False

@pytest.mark.timeout(300, func_only=True)
def test_bucket_trailing_slash_PBM_315(start_cluster, cluster):
    """
    Test that PBM handles a bucket name with a trailing forward slash.
    """
    result = cluster.exec_pbm_cli("config --set storage.s3.bucket=bcp/ --wait")
    assert result.rc == 0, f"Failed to set bucket with trailing slash"

    backup = cluster.make_backup("logical")
    Cluster.log(f"Backup completed: {backup}")

    assert check_backup_exists(cluster, backup), f"Backup {backup} does not exist"

@pytest.mark.timeout(300, func_only=True)
def test_bucket_preceding_slash_PBM_316(start_cluster, cluster):
    """
    Test that PBM handles a prefix with a preceding forward slash.
    """
    result = cluster.exec_pbm_cli("config --set storage.s3.prefix=/pbme2etest storage.s3.bucket=bcp/ --wait")
    assert result.rc == 0, f"Failed to set prefix with preceding slash"

    backup = cluster.make_backup("logical")
    Cluster.log(f"Backup completed: {backup}")

    assert check_backup_exists(cluster, backup), f"Backup {backup} does not exist"

    cluster.make_resync()

    assert check_backup_exists(cluster, backup), f"Backup {backup} does not exist"
