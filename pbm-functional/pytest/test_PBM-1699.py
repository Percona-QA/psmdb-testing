import json
import os
import re
import time
from datetime import datetime, timezone

import pymongo
import pytest
from cluster import Cluster

DURATION_RE = re.compile(r"^(?:\d+h)?(?:\d+m)?\d+s$")
TIMING_TOLERANCE_SEC = 5


def _parse_rfc3339(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _snapshot(items, backup_name):
    for item in items:
        if item.get("name") == backup_name:
            return item
    return None


def _wait_backup_done(cluster, backup, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = _snapshot(cluster.get_status().get("backups", {}).get("snapshot", []), backup)
        if snap and snap.get("status") == "done":
            return
        time.sleep(1)
    assert False, f"backup {backup} did not reach done"


def _now():
    return datetime.now(timezone.utc)


def _assert_within(reported, observed, label):
    delta = abs((reported - observed).total_seconds())
    Cluster.log(
        f"{label}: reported {reported.isoformat()} vs observed {observed.isoformat()} "
        f"(delta {delta:.1f}s, tolerance {TIMING_TOLERANCE_SEC}s)"
    )
    assert delta <= TIMING_TOLERANCE_SEC, (
        f"{label}: reported {reported.isoformat()} vs observed {observed.isoformat()} "
        f"(delta {delta:.1f}s, tolerance {TIMING_TOLERANCE_SEC}s)"
    )


def _take_backup(cluster, backup_type):
    if backup_type == "incremental":
        cluster.make_backup("incremental --base")
    started = _now()
    if backup_type == "external":
        backup = cluster.external_backup_start()
        cluster.external_backup_copy(backup)
        cluster.external_backup_finish(backup)
        _wait_backup_done(cluster, backup)
    elif backup_type == "incremental":
        backup = cluster.make_backup("incremental")
    else:
        backup = cluster.make_backup(backup_type)
    return backup, started, _now()


def _assert_backup_timing(cluster, backup, started, finished):
    result = cluster.exec_pbm_cli(f"describe-backup {backup} --out=json")
    assert result.rc == 0, result.stdout + result.stderr
    desc = json.loads(result.stdout)
    Cluster.log(f"describe-backup {backup}: {desc}")

    assert desc.get("start"), f"describe-backup is missing start: {desc}"
    assert desc.get("finish"), f"describe-backup is missing finish: {desc}"
    start = _parse_rfc3339(desc["start"])
    finish = _parse_rfc3339(desc["finish"])
    assert finish >= start, f"finish {desc['finish']} is before start {desc['start']}"
    _assert_within(start, started, "start")
    _assert_within(finish, finished, "finish")
    duration = desc.get("duration", "")
    if finish > start:
        assert DURATION_RE.fullmatch(duration), f"unexpected duration {duration!r}: {desc}"
    else:
        assert duration == "", f"same-second backup should have empty duration: {desc}"

    result = cluster.exec_pbm_cli("list")
    assert result.rc == 0, result.stdout + result.stderr
    assert "DURATION" in result.stdout, f"pbm list is missing DURATION column:\n{result.stdout}"

    result = cluster.exec_pbm_cli("list --out=json")
    assert result.rc == 0, result.stdout + result.stderr
    listed = _snapshot(json.loads(result.stdout).get("snapshots", []), backup)
    assert listed, f"backup {backup} not found in pbm list"
    assert listed.get("duration", "") == duration, f"list duration mismatch: {listed}"

    result = cluster.exec_pbm_cli("status")
    assert result.rc == 0, result.stdout + result.stderr
    assert "DURATION" in result.stdout, f"pbm status is missing DURATION column:\n{result.stdout}"

    status_snap = _snapshot(cluster.get_status().get("backups", {}).get("snapshot", []), backup)
    assert status_snap, f"backup {backup} not found in pbm status"
    assert status_snap.get("duration", "") == duration, f"status duration mismatch: {status_snap}"

    if duration:
        result = cluster.exec_pbm_cli("logs -t0")
        assert result.rc == 0, result.stdout + result.stderr
        expected = f"backup: {backup}, start: {desc['start']}, finish: {desc['finish']}, duration: {duration}"
        assert expected in result.stdout, f"timing summary not found in pbm logs:\n{result.stdout}"


@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}]},
        ],
    }


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm("/etc/pbm-fs.conf")
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.data", key={"x": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.jenkins
@pytest.mark.parametrize("backup_type", ["logical", "physical", "incremental", "external"])
@pytest.mark.timeout(600, func_only=True)
def test_backup_timing_visibility_PBM_T369(start_cluster, cluster, backup_type):
    """
    PBM-1699: completed backup on a sharded cluster reports start, finish
    and duration in describe-backup, list, status and logs.
    """
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["data"].insert_many([{"x": i, "pad": "x" * 1024} for i in range(20000)])

    backup, started, finished = _take_backup(cluster, backup_type)
    _assert_backup_timing(cluster, backup, started, finished)
    Cluster.log("Finished successfully")
