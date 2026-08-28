import json
import os
import time

import pymongo
import pytest
import testinfra
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
        cluster.destroy(cleanup_backups=True)
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm("/etc/pbm-fs.conf")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


def _assert_describe_backup_ok(cluster, backup_name, expected_status):
    result = cluster.exec_pbm_cli(f"describe-backup {backup_name} --out=json")
    assert result.rc == 0, (
        f"describe-backup failed. Expected: {expected_status} backup: rc={result.rc} "
        f"stdout={result.stdout} stderr={result.stderr}"
    )

    descr = json.loads(result.stdout)
    assert descr.get("name") == backup_name, f"Unexpected describe-backup payload: {descr}"
    assert descr.get("status") == expected_status, f"Unexpected status: {descr}"

    # Plain-text output should behave the same way as --out=json
    result_text = cluster.exec_pbm_cli(f"describe-backup {backup_name}")
    assert result_text.rc == 0, (
        f"describe-backup (text) failed for a zero-size backup: rc={result_text.rc} "
        f"stdout={result_text.stdout} stderr={result_text.stderr}"
    )
    assert "missed file" not in (result_text.stdout + result_text.stderr), (
        f"Regression in plain-text output: {result_text.stdout} {result_text.stderr}"
    )


@pytest.mark.timeout(180, func_only=True)
def test_describe_backup_on_canceled_backup_PBM_370(start_cluster, cluster):
    """
    Verify that an immediately cancelled backup's
    describe-backup will show the correct output
    """
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many([{"x": i} for i in range(1000)])

    result = cluster.exec_pbm_cli("backup --type=logical --out=json")
    assert result.rc == 0, f"Failed to start backup: {result.stdout} {result.stderr}"
    backup_name = json.loads(result.stdout)["name"]

    cancel = cluster.exec_pbm_cli("cancel-backup")
    assert cancel.rc == 0, f"cancel-backup failed: {cancel.stdout} {cancel.stderr}"

    timeout = time.time() + 60
    while True:
        status = cluster.get_status()
        snapshots = status.get("backups", {}).get("snapshot", [])
        matching = [s for s in snapshots if s["name"] == backup_name]
        if matching and matching[0]["status"] == "canceled":
            break
        assert time.time() < timeout, "Timed out waiting for backup to be canceled"
        time.sleep(1)

    n = testinfra.get_host("docker://rs101")
    dump_files = n.check_output(f"find /backups/{backup_name} -type f 2>/dev/null || true")
    assert dump_files.strip() == "", (
        f"Expected no dump files to have been written before cancellation, found: {dump_files}"
    )

    _assert_describe_backup_ok(cluster, backup_name, "canceled")
    Cluster.log("Finished successfully")


@pytest.mark.timeout(3600, func_only=True)
def test_describe_backup_on_agent_lost_backup_PBM_371(start_cluster, cluster):
    """
    Verify that a backup that fails due to the pbm-agent dying the
    describe-backup will show the correct output
    """
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many([{"x": i} for i in range(1000)])

    result = cluster.exec_pbm_cli("backup --type=logical --out=json")
    assert result.rc == 0, f"Failed to start backup: {result.stdout} {result.stderr}"
    backup_name = json.loads(result.stdout)["name"]

    n = testinfra.get_host("docker://rs101")
    n.check_output("kill -9 $(pgrep pbm-agent)")
    time.sleep(35)

    Cluster.restart_pbm_agent("rs101")
    cluster.check_pbm_status()

    result2 = cluster.exec_pbm_cli("backup --type=logical --out=json")
    assert result2.rc == 0, f"Failed to start second backup: {result2.stdout} {result2.stderr}"

    timeout = time.time() + 120
    while True:
        status = cluster.get_status()
        snapshots = status.get("backups", {}).get("snapshot", [])
        matching = [s for s in snapshots if s["name"] == backup_name]
        if matching and matching[0]["status"] == "error":
            break
        assert time.time() < timeout, (
            f"Timed out waiting for the abandoned backup to be marked as error: {matching}"
        )
        time.sleep(1)

    Cluster.log(f"Backup {backup_name} ended with status {matching[0]['status']}: {matching[0].get('error')}")
    assert "lost" in matching[0].get("error", "").lower(), (
        f"Expected an agent-lost error message, got: {matching[0].get('error')}"
    )

    _assert_describe_backup_ok(cluster, backup_name, "error")
