import docker
from datetime import datetime, timezone

import pymongo
import pytest

from cluster import Cluster

SHARD_PRIMARIES = ["rs1668a", "rs1668d"]

@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos1668",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg1668a"}, {"host": "rscfg1668b"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs1668a"}, {"host": "rs1668b"}, {"host": "rs1668c"}]},
            {"_id": "rs2", "members": [{"host": "rs1668d"}, {"host": "rs1668e"}, {"host": "rs1668f"}]},
        ],
    }

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="package")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.col1", key={"field": "hashed"})
        client["test"]["col1"].insert_many([{"field": i} for i in range(1000)])
        client["test"]["col1"].create_index("field", name="test_index")
        client.close()
        yield
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.fixture(scope="package")
def logical_backup(cluster, start_cluster):
    return cluster.make_backup("logical")

@pytest.fixture(scope="function")
def reset_state(cluster, logical_backup):
    cluster.exec_pbm_cli("config --set restore.indexCommitQuorum= --wait")
    yield

def _check_mongodb_logs(expected_value, since):
    """Assert commitQuorum=expected_value in docker logs on every shard primary after `since`."""
    try:
        int(expected_value)
        expected_fragment = f'"commitQuorum":{expected_value}'
    except ValueError:
        expected_fragment = f'"commitQuorum":"{expected_value}"'

    docker_client = docker.from_env()
    for container_name in SHARD_PRIMARIES:
        container = docker_client.containers.get(container_name)
        raw_logs = container.logs(since=int(since.timestamp())).decode("utf-8", errors="replace")
        commit_quorum_logs = [line for line in raw_logs.splitlines() if "commitQuorum" in line]
        assert commit_quorum_logs, f"No commitQuorum log entries found on {container_name} after {since}"
        for line in commit_quorum_logs:
            assert expected_fragment in line, (
                f"Expected commitQuorum={expected_value} on {container_name}, got: {line}"
            )

@pytest.mark.timeout(300, func_only=True)
def test_majority_quorum_sharded_PBM_T1668(reset_state, cluster, logical_backup):
    """indexCommitQuorum=majority is applied across all shards during logical restore."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=majority --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=majority: {result.stderr}"

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(logical_backup, check_pbm_status=True)
    _check_mongodb_logs("majority", since=restore_time)
