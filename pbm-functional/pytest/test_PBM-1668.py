import json
from datetime import datetime, timezone
from time import sleep

import pymongo
import pytest
import testinfra

from cluster import Cluster


@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="package")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
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
        client["test"][col].create_index("field", name="test_index", unique=True)
    client.close()

def _check_mongodb_logs(expected_value, since):
    n = testinfra.get_host("docker://rs101")
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
    assert commit_quorum_logs, f"No commitQuorum log entries found after {since}"
    # integers arrive in logs without quotes: "commitQuorum":1
    # strings arrive with quotes: "commitQuorum":"votingMembers"
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
def test_default_quorum_PBM_T1668(reset_state, cluster):
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
def test_explicit_voting_members_quorum_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=votingMembers --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=votingMembers: {result.stderr}"

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum") == "votingMembers", (
        f"Expected indexCommitQuorum=votingMembers in config, got: {config_data.get('restore')}"
    )

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, check_pbm_status=True)

    _check_mongodb_logs("votingMembers", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
def test_majority_quorum_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=majority --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=majority: {result.stderr}"

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum") == "majority", (
        f"Expected indexCommitQuorum=majority in config, got: {config_data.get('restore')}"
    )

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, check_pbm_status=True)

    _check_mongodb_logs("majority", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("value,expected_error", [
    ("Testing",    'invalid restore.indexCommitQuorum "Testing"'),
    ("0",          'invalid restore.indexCommitQuorum "0"'),
    ("-1",         'invalid restore.indexCommitQuorum "-1"'),
    ("1-1",        'invalid restore.indexCommitQuorum "1-1"'),
    ('" majority"', "must not contain leading or trailing whitespace"),
    ('"majority "', "must not contain leading or trailing whitespace"),
    ('" 1"',       "must not contain leading or trailing whitespace"),
    ("none",       'invalid restore.indexCommitQuorum "none"'),
    ("1.5",        'invalid restore.indexCommitQuorum "1.5"'),
    ("nil",        'invalid restore.indexCommitQuorum "nil"'),
    ("nullptr",    'invalid restore.indexCommitQuorum "nullptr"'),
    ("null",       'invalid restore.indexCommitQuorum "null"'),
    pytest.param(
        "51", 'invalid restore.indexCommitQuorum "51"',
        marks=pytest.mark.xfail(reason="PBM bug: integers > 50 not rejected at config validation time"),
    ),
])
def test_invalid_quorum_config_PBM_T1668(reset_state, cluster, value, expected_error):
    result = cluster.exec_pbm_cli(f"config --set restore.indexCommitQuorum={value} --wait")
    assert result.rc != 0, f"Expected failure for {value!r} but rc=0. stdout: {result.stdout}"
    assert expected_error in result.stderr, (
        f"Expected {expected_error!r} for {value!r}, got stderr: {result.stderr}"
    )

@pytest.mark.timeout(300, func_only=True)
def test_integer_quorum_config_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=1 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=1: {result.stderr}"

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum") == "1", (
        f"Expected indexCommitQuorum=1 in config, got: {config_data.get('restore')}"
    )

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, check_pbm_status=True)

    _check_mongodb_logs("1", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
def test_quorum_exceeds_node_count_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=50 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=50: {result.stderr}"

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum") == "50", (
        f"Expected indexCommitQuorum=50 in config, got: {config_data.get('restore')}"
    )

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")

    n = testinfra.get_host("docker://rs101")
    result = n.run(f"SSL_CERT_FILE=/etc/nginx-minio/ca.crt pbm restore {backup} -y --wait")
    assert result.rc != 0, f"Expected restore to fail but rc=0. stdout: {result.stdout}"
    assert "UnsatisfiableCommitQuorum" in result.stdout + result.stderr, (
        f"Expected UnsatisfiableCommitQuorum error, got stdout: {result.stdout}, stderr: {result.stderr}"
    )

    cluster.wait_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=3 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=3: {result.stderr}"

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, check_pbm_status=True)

    _check_mongodb_logs("3", since=restore_time)

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("quorum", ["2", "3"])
def test_valid_integer_quorum_PBM_T1668(reset_state, cluster, quorum):
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli(f"config --set restore.indexCommitQuorum={quorum} --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum={quorum}: {result.stderr}"

    config_result = cluster.exec_pbm_cli("config --out json")
    config_data = json.loads(config_result.stdout)
    assert config_data.get("restore", {}).get("indexCommitQuorum") == quorum, (
        f"Expected indexCommitQuorum={quorum} in config, got: {config_data.get('restore')}"
    )

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, check_pbm_status=True)

    _check_mongodb_logs(quorum, since=restore_time)

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("quorum", ["majority", "votingMembers", "1", "2", "3"])
def test_cli_flag_valid_values_PBM_T1668(reset_state, cluster, quorum):
    cluster.check_pbm_status()

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, restore_opts=["--index-commit-quorum", quorum])

    _check_mongodb_logs(quorum, since=restore_time)

@pytest.mark.timeout(300, func_only=True)
def test_cli_flag_exceeds_node_count_PBM_T1668(reset_state, cluster):
    cluster.check_pbm_status()

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")

    result = cluster.make_restore(backup, restore_opts=["--index-commit-quorum", "4"], expect_failure=True)
    assert result.rc != 0, f"Expected restore to fail but rc=0. stdout: {result.stdout}"
    assert "UnsatisfiableCommitQuorum" in result.stdout + result.stderr, (
        f"Expected UnsatisfiableCommitQuorum error, got stdout: {result.stdout}, stderr: {result.stderr}"
    )

    cluster.wait_pbm_status()

@pytest.mark.timeout(7200, func_only=True)
@pytest.mark.parametrize("quorum,expected_error", [
    ("0",          'invalid restore.indexCommitQuorum "0"'),
    ("-1",         'invalid restore.indexCommitQuorum "-1"'),
    ("testing",    'invalid restore.indexCommitQuorum "testing"'),
    ('" majority"', "must not contain leading or trailing whitespace"),
])
def test_cli_flag_invalid_values_PBM_T1668(reset_state, cluster, quorum, expected_error):
    result = cluster.exec_pbm_cli(f"restore test -y --index-commit-quorum {quorum} --wait")
    assert result.rc != 0, f"Expected failure for {quorum!r} but rc=0. stdout: {result.stdout}"
    assert expected_error in result.stdout + result.stderr, (
        f"Expected {expected_error!r} for {quorum!r}, got: {result.stdout}{result.stderr}"
    )

@pytest.mark.timeout(300, func_only=True)
def test_cli_flag_overrides_config_PBM_T1668(reset_state, cluster):
    """CLI --index-commit-quorum flag takes precedence over restore.indexCommitQuorum config."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=1 --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=1: {result.stderr}"

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("logical")
    restore_time = datetime.now(timezone.utc)
    cluster.make_restore(backup, restore_opts=["--index-commit-quorum", "2"])

    _check_mongodb_logs("2", since=restore_time)

@pytest.mark.timeout(600, func_only=True)
def test_pitr_restore_applies_index_commit_quorum_PBM_T1668(reset_state, cluster):
    """indexCommitQuorum config is applied during the base snapshot phase of a PITR restore.

    PITR() calls RunSnapshot() which rebuilds indexes with the configured commitQuorum,
    so the behaviour is identical to a plain logical restore.
    """
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=majority --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=majority: {result.stderr}"

    _setup_indexed_data(cluster)
    cluster.make_backup("logical")

    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["pitr_col"].insert_many([{"val": i} for i in range(100)])
    client.close()
    sleep(10)
    pitr = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    sleep(10)
    cluster.disable_pitr(pitr)

    restore_time = datetime.now(timezone.utc)
    cluster.make_restore("--time=" + pitr, check_pbm_status=True)

    _check_mongodb_logs("majority", since=restore_time)

    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["col1"].count_documents({}) == 1000, "Snapshot data missing after PITR restore"
    assert client["test"]["pitr_col"].count_documents({}) == 100, "Oplog-replayed data missing after PITR restore"
    client.close()

@pytest.mark.timeout(600, func_only=True)
def test_physical_restore_ignores_index_commit_quorum_PBM_T1668(reset_state, cluster):
    """indexCommitQuorum config is not applied during physical restore (no index rebuild)."""
    cluster.check_pbm_status()

    result = cluster.exec_pbm_cli("config --set restore.indexCommitQuorum=majority --wait")
    assert result.rc == 0, f"Failed to set indexCommitQuorum=majority: {result.stderr}"

    _setup_indexed_data(cluster)
    backup = cluster.make_backup("physical")
    cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)

    # Physical restore copies raw data files — MongoDB never rebuilds indexes, so
    # commitQuorum must not appear in logs regardless of the config value.
    n = testinfra.get_host("docker://rs101")
    log_result = n.run(
        'mongosh -u root -p root --quiet --eval '
        '"db.adminCommand({getLog:\'global\'}).log.forEach(x => print(x))"'
    )
    commit_quorum_logs = [line for line in log_result.stdout.splitlines() if "commitQuorum" in line]
    assert not commit_quorum_logs, (
        f"Expected no commitQuorum log entries after physical restore, found: {commit_quorum_logs}"
    )

