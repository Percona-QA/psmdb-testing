import pytest
import os
import docker

from cluster import Cluster


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()


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
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize(
    "command, output",
    [
        ("restore", "Error: specify a backup name, --time, or --external"),
        (
            "restore restore 2025-08-01T15:00:49Z --time 2025-08-01T15:00:00",
            "Error: backup name and --time cannot be used together",
        ),
    ],
)
def test_logical_PBM_T298(start_cluster, cluster, command, output):
    """
    A test to verify correct validation message is displayed if restore command is incorrectly used.
    """
    cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio:15380 --wait")
    result = cluster.exec_pbm_cli(command)
    assert result.stderr.strip() == output
