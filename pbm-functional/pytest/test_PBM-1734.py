import json
import time

import pymongo
import pytest
import testinfra
from cluster import Cluster


@pytest.fixture(scope="function")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}


@pytest.fixture(scope="function")
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


@pytest.mark.timeout(3600, func_only=True)
def test_no_s3_debug_leak_in_status_PBM_T371(start_cluster, cluster):
    """
    Verify no S3 output leaks into pbm status, pbm status --out=json, and pbm describe-backup
    """
    n = testinfra.get_host("docker://" + cluster.pbm_cli)

    result = cluster.exec_pbm_cli("config --set storage.s3.debugLogLevels=Request,Retries --wait")
    assert result.rc == 0, f"Failed to enable S3 debug logging: {result.stdout} {result.stderr}"

    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many([{"x": i} for i in range(1000)])

    start = cluster.exec_pbm_cli("backup --out=json")
    assert start.rc == 0, f"Failed to start backup: {start.stdout} {start.stderr}"
    backup_name = json.loads(start.stdout)["name"]

    bcp_coll = client["admin"]["pbmBackups"]
    timeout = time.time() + 10
    while True:
        doc = bcp_coll.find_one({"name": backup_name})
        if doc and any(rs.get("dump_name") for rs in doc.get("replsets", [])):
            break
        assert time.time() < timeout, (
            f"Backup {backup_name} never recorded a replset dump_name in time: {doc}"
        )
        time.sleep(0.05)

    n.check_output("kill -9 $(pgrep pbm-agent)")
    time.sleep(35)

    Cluster.restart_pbm_agent(cluster.pbm_cli)
    cluster.wait_pbm_status()

    trigger = cluster.exec_pbm_cli("backup --out=json")
    assert trigger.rc == 0, f"Failed to start trigger backup: {trigger.stdout} {trigger.stderr}"
    trigger_name = json.loads(trigger.stdout)["name"]

    timeout = time.time() + 60
    while True:
        status = cluster.get_status()
        snapshots = status.get("backups", {}).get("snapshot", [])
        found = [s for s in snapshots if s["name"] == backup_name]
        if found and found[0]["status"] in ("error", "done", "canceled"):
            matching = found[0]
            break
        assert time.time() < timeout, f"Timed out waiting for {backup_name} to reach a terminal status"
        time.sleep(1)

    timeout = time.time() + 60
    while True:
        status = cluster.get_status()
        snapshots = status.get("backups", {}).get("snapshot", [])
        found = [s for s in snapshots if s["name"] == trigger_name]
        if found and found[0]["status"] in ("error", "done", "canceled"):
            break
        assert time.time() < timeout, f"Timed out waiting for trigger backup {trigger_name} to finish"
        time.sleep(1)

    assert matching["status"] == "error", (f"Backup {backup_name} didn't end up errored (got {matching['status']!r})")

    failures = []
    S3_DEBUG_MARKERS = ["aws4-hmac-sha256", "x-amz-date", "x-amz-content-sha256", "aws-sdk-go-v2"]
    for cmd in ("pbm status", "pbm status --out=json", f"pbm describe-backup {backup_name}"):
        Cluster.log(f"Running: {cmd}")
        result = n.run(cmd)
        combined = result.stdout + "\n" + result.stderr
        combined_lower = combined.lower()
        leaked = [marker for marker in S3_DEBUG_MARKERS if marker in combined_lower]
        if leaked:
            failures.append(
                f"'{cmd}' leaked (found: {leaked}):\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
        elif result.rc != 0:
            failures.append(
                f"'{cmd}' failed (rc={result.rc})\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )

    assert not failures, (
        f"{len(failures)}/3 commands failed the check (leaked debug output and/or exited "
        f"non-zero):\n\n" + "\n\n".join(failures)
    )