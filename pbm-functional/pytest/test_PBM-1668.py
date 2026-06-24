import json
from datetime import datetime, timezone
from time import sleep

import pymongo
import pytest
import testinfra

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs1668a"}, {"host": "rs1668b"}, {"host": "rs1668c"}]}

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
        client["test"]["col1"].insert_many([{"field": i} for i in range(1000)])
        client["test"]["col1"].create_index("field", name="test_index", unique=True)
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
    pymongo.MongoClient(cluster.connection).drop_database("test")
    yield

def _setup_indexed_data(cluster):
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["col1"].insert_many([{"field": i} for i in range(1000)])
    client["test"]["col1"].create_index("field", name="test_index", unique=True)
    client.close()

def _check_mongodb_logs(expected_value, since):
    n = testinfra.get_host("docker://rs1668a")
    log_result = n.run(
        'mongo -u root -p root --quiet --eval '
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
    assert commit_quorum_logs, f"No commitQuorum log entries found after {since}"
    try:
        int(expected_value)
        expected_fragment = f'"commitQuorum":{expected_value}'
    except ValueError:
        expected_fragment = f'"commitQuorum":"{expected_value}"'
    for line in commit_quorum_logs:
        assert expected_fragment in line, (
            f"Expected commitQuorum={expected_value} in log line, got: {line}"
        )

@pytest.mark.timeout(300, func_only=True)
def test_default_quorum_PBM_T336(reset_state, cluster, logical_backup):
    """Verify default commitQuorum setting is votingMembers."""
    cluster.check_pbm_status()

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum", "") == "", (
        f"Expected indexCommitQuorum to not appear, got: {config_data.get('restore')}"
    )

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(logical_backup, check_pbm_status=True)

    _check_mongodb_logs("votingMembers", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
def test_explicit_voting_members_quorum_PBM_T337(reset_state, cluster, logical_backup):
    """Verify indexCommitQuorum=votingMembers applies votingMembers during restore."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=votingMembers --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=votingMembers: {result.stderr}"

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum") == "votingMembers", (
        f"Expected indexCommitQuorum=votingMembers in config, got: {config_data.get('restore')}"
    )

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(logical_backup, check_pbm_status=True)

    _check_mongodb_logs("votingMembers", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
def test_majority_quorum_PBM_T338(reset_state, cluster, logical_backup):
    """Verify indexCommitQuorum=majority applies majority during restore."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=majority --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=majority: {result.stderr}"

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum") == "majority", (
        f"Expected indexCommitQuorum=majority in config, got: {config_data.get('restore')}"
    )

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(logical_backup, check_pbm_status=True)

    _check_mongodb_logs("majority", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("value,expected_error", [
    ("Testing",    'invalid restore.indexCommitQuorum "Testing"'),
    ("0",          'invalid restore.indexCommitQuorum "0"'),
    ('" majority"', "must not contain leading or trailing whitespace"),
    ('"majority "', "must not contain leading or trailing whitespace"),
    ("51",         'invalid restore.indexCommitQuorum "51"')
])
def test_invalid_quorum_config_PBM_T339(start_cluster, cluster, value, expected_error):
    """Verify restore.indexCommitQuorum config validation for rejected values"""
    cluster.check_pbm_status()
    result = cluster.exec_pbm_cli(f"config --set restore.indexCommitQuorum={value} --wait")
    assert result.rc != 0, f"Expected failure for {value!r} but rc=0. stdout: {result.stdout}"
    assert expected_error in result.stderr, (
        f"Expected {expected_error!r} for {value!r}, got stderr: {result.stderr}"
    )

@pytest.mark.timeout(300, func_only=True)
def test_integer_quorum_config_PBM_T340(reset_state, cluster, logical_backup):
    """Verify a positive integer is accepted and applied during restore."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=2 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=2: {result.stderr}"

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum") == "2", (
        f"Expected indexCommitQuorum=2 in config, got: {config_data.get('restore')}"
    )

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(logical_backup, check_pbm_status=True)

    _check_mongodb_logs("2", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
def test_quorum_exceeds_node_count_PBM_T341(reset_state, cluster, logical_backup):
    """Verify An integer quorum exceeding voting members falls back to votingMembers and restore succeeds."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=50 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=50: {result.stderr}"

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(logical_backup, check_pbm_status=True)

    logs_result = cluster.exec_pbm_cli("logs -sW -t0 -e restore")
    assert "index commit quorum cannot be satisfied for test.col1, retrying with votingMembers" in logs_result.stdout, (
        f"Expected fallback warning in PBM logs, got: {logs_result.stdout}"
    )

    _check_mongodb_logs("votingMembers", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
def test_cli_flag_valid_values_PBM_T342(reset_state, cluster, logical_backup):
    """Verify valid --index-commit-quorum CLI values are accepted and applied during restore."""
    cluster.check_pbm_status()

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(logical_backup, restore_opts=["--index-commit-quorum", "majority"])

    _check_mongodb_logs("majority", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
def test_cli_flag_overrides_config_PBM_T343(reset_state, cluster, logical_backup):
    """Verify CLI --index-commit-quorum flag takes precedence over config option."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=1 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=1: {result.stderr}"

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(logical_backup, restore_opts=["--index-commit-quorum", "2"])

    _check_mongodb_logs("2", since=restore_time)

@pytest.mark.timeout(600, func_only=True)
def test_pitr_restore_applies_index_commit_quorum_PBM_T344(reset_state, cluster):
    """Verify indexCommitQuorum config is applied during the base snapshot phase of a PITR restore."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=majority --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=majority: {result.stderr}"

    _setup_indexed_data(cluster)
    cluster.make_backup("logical")

    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["pitr_col"].insert_many([{"val": i} for i in range(100)])
    client.close()
    sleep(5)
    pitr = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    sleep(5)
    cluster.disable_pitr(pitr)

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore("--time=" + pitr, check_pbm_status=True)

    _check_mongodb_logs("majority", since=restore_time)

    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["col1"].count_documents({}) == 1000, "Snapshot data missing after PITR restore"
    assert client["test"]["pitr_col"].count_documents({}) == 100, "Oplog-replayed data missing after PITR restore"
    client.close()

@pytest.mark.timeout(600, func_only=True)
def test_physical_restore_ignores_index_commit_quorum_PBM_T345(reset_state, cluster):
    """Verify indexCommitQuorum config is not applied during a physical restore"""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=majority --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=majority: {result.stderr}"

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("physical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)

    n = testinfra.get_host("docker://rs1668a")
    log_result = n.run(
        'mongo -u root -p root --quiet --eval '
        '"db.adminCommand({getLog:\'global\'}).log.forEach(x => print(x))"'
    )
    commit_quorum_logs = []
    for line in log_result.stdout.splitlines():
        if "commitQuorum" not in line:
            continue
        try:
            log_time = datetime.fromisoformat(json.loads(line)["t"]["$date"])
            if log_time >= restore_time:
                commit_quorum_logs.append(line)
        except (json.JSONDecodeError, KeyError, ValueError):
            pass
    assert not commit_quorum_logs, (
        f"Expected no commitQuorum log entries after physical restore, found: {commit_quorum_logs}"
    )
