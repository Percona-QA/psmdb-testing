import json
from datetime import datetime, timezone

import pymongo
import pytest
import testinfra

from cluster import Cluster


SHARD_PRIMARIES = ["rs101", "rs201"]


@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}, {"host": "rscfg02"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}]},
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
        for col in ["col1", "col2", "col3"]:
            client.admin.command("shardCollection", f"test.{col}", key={"field": "hashed"})
        client.close()
        yield
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.fixture(scope="function")
def reset_state(cluster, start_cluster):
    cluster.exec_pbm_cli("config --set restore.indexCommitQuorum= --wait")
    pymongo.MongoClient(cluster.connection).drop_database("test")
    yield


def _setup_indexed_data(cluster):
    client = pymongo.MongoClient(cluster.connection)
    for col in ["col1", "col2", "col3"]:
        client["test"][col].insert_many([{"field": i} for i in range(1000)])
        client["test"][col].create_index("field", name="test_index")
    client.close()


def _check_mongodb_logs(expected_value, since):
    """Assert commitQuorum=expected_value in logs on every shard primary after `since`."""
    try:
        int(expected_value)
        expected_fragment = f'"commitQuorum":{expected_value}'
    except ValueError:
        expected_fragment = f'"commitQuorum":"{expected_value}"'

    for host in SHARD_PRIMARIES:
        n = testinfra.get_host(f"docker://{host}")
        log_result = n.run(
            'mongosh -u root -p root --quiet --eval '
            '"db.adminCommand({getLog:\'global\'}).log.forEach(x => print(x))"'
        )
        commit_quorum_logs = []
        for line in log_result.stdout.splitlines():
            if "commitQuorum" not in line:
                continue
            try:
                log_time = datetime.fromisoformat(json.loads(line)["t"]["$date"])
                if log_time >= since:
                    commit_quorum_logs.append(line)
            except (json.JSONDecodeError, KeyError, ValueError):
                pass
        assert commit_quorum_logs, f"No commitQuorum log entries found on {host} after {since}"
        for line in commit_quorum_logs:
            assert expected_fragment in line, (
                f"Expected commitQuorum={expected_value} on {host}, got: {line}"
            )


@pytest.mark.timeout(300, func_only=True)
def test_default_quorum_sharded_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum", "") == "", (
        f"Expected indexCommitQuorum to not appear, got: {config_data.get('restore')}"
    )

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, check_pbm_status=True)

    _check_mongodb_logs("votingMembers", since=restore_time)


@pytest.mark.timeout(300, func_only=True)
def test_majority_quorum_sharded_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=majority --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=majority: {result.stderr}"

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, check_pbm_status=True)

    _check_mongodb_logs("majority", since=restore_time)


@pytest.mark.timeout(300, func_only=True)
def test_integer_quorum_sharded_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=1 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=1: {result.stderr}"

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, check_pbm_status=True)

    _check_mongodb_logs("1", since=restore_time)


@pytest.mark.timeout(300, func_only=True)
def test_cli_flag_overrides_config_sharded_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=1 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=1: {result.stderr}"

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, restore_opts=["--index-commit-quorum", "majority"])

    _check_mongodb_logs("majority", since=restore_time)
