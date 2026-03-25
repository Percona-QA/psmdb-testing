import os

import pymongo
import pytest
import testinfra

from cluster import Cluster

documents = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]


@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config, no_auth=True, pbm_mongodb_uri="mongodb://rs101:27017/")


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(300, func_only=True)
def test_physical_restore_non_localhost_no_auth_PBM_T321(start_cluster, cluster):
    """
    Verify that when a physical restore is performed and the shutdown is rejected by MongoDB running without auth, due to a
    non-localhost connection, the command does not hang and should exit with an unauthorized error
    """

    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("physical")

    host = testinfra.get_host("docker://rs101")

    result = host.run("timeout 120 pbm restore " + backup + " --wait")

    assert result.rc != 0, (
        "Expected unauthorized error within 2 minutes"
    )
    error_output = result.stdout + result.stderr
    assert "unauthorized" in error_output.lower(), (
        f"Expected 'Unauthorized' in output.\nActual output\n: STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
