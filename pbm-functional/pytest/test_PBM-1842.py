import concurrent.futures
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

import pymongo
import pytest
import testinfra
from cluster import Cluster


@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}


@pytest.fixture(scope="package")
def cluster(config):
    # rseq workaround: PSMDB 8.x won't start on kernel 6.19+ otherwise (SERVER-121912)
    return Cluster(config, extra_environment={"GLIBC_TUNABLES": "glibc.pthread.rseq=1"})


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

@pytest.mark.timeout(900, func_only=True)
def test_logical_pitr_expiring_timeseries_ttl_off_restore(start_cluster, cluster):
    """ Verify logical PITR restore of an expiring timeseries collection works when TTL deletes buckets during backup """
    client = pymongo.MongoClient(cluster.connection)
    nodes = {m["host"]: pymongo.MongoClient(f"mongodb://root:root@{m['host']}:27017/?directConnection=true")
             for m in cluster.config["members"]}
    for node in nodes.values():
        node.admin.command({"setParameter": 1, "ttlMonitorSleepSecs": 1})
    client["test"].create_collection("ts1", timeseries={"timeField": "timestamp"}, expireAfterSeconds=30)

    stop_event = threading.Event()
    inserted = [0]

    def writer():
        local_client = pymongo.MongoClient(cluster.connection)
        coll = local_client["test"]["ts1"]
        while not stop_event.is_set():
            coll.insert_one({"timestamp": datetime.now(tz=timezone.utc) - timedelta(hours=2),
                             "x": random.randint(0, 1 << 30)})
            inserted[0] += 1
        local_client.close()

    executor = concurrent.futures.ThreadPoolExecutor()
    future = executor.submit(writer)
    time.sleep(5)

    # Sanity check: TTL really is deleting buckets, otherwise the test proves nothing
    present = client["test"]["ts1"].count_documents({})
    Cluster.log(f"Inserted {inserted[0]} docs so far, {present} still present")
    assert present < inserted[0] / 2, "TTL monitor is not deleting buckets"

    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    Cluster.log("Generating expiring timeseries data for 15 seconds")
    time.sleep(15)
    stop_event.set()
    future.result()
    executor.shutdown()
    cluster.disable_pitr()
    time.sleep(6)

    pitr_end = cluster.get_last_pitr_chunk_end()
    pitr_time = datetime.fromtimestamp(pitr_end, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    for node in nodes.values():
        node.admin.command({"setParameter": 1, "ttlMonitorEnabled": False})
    # Wait until the TTL pass count stays unchanged for 5s on every node, so the monitor has really stopped
    timeout = time.time() + 60
    stable_since = time.time()
    passes = {}
    while time.time() - stable_since < 5:
        current = {h: n.admin.command("serverStatus")["metrics"]["ttl"]["passes"] for h, n in nodes.items()}
        if current != passes:
            passes = current
            stable_since = time.time()
        assert time.time() < timeout, f"TTL monitor still running: passes={passes}"
        time.sleep(1)
    Cluster.log(f"TTL monitor confirmed stopped on all nodes (passes={passes})")
    before = client["test"]["ts1"].count_documents({})

    client["test"].drop_collection("ts1")
    try:
        cluster.make_restore("--time=" + pitr_time, check_pbm_status=True)
    except AssertionError as e:
        error = str(e)
        assert "Location6781400" in error and "missing 'control' field" in error \
            and "system.buckets.ts1" in error, f"Restore failed with an unexpected error: {error}"
        pytest.fail("Restore failed: replayed an update for a timeseries bucket deleted by TTL "
                    "during the backup (Location6781400, missing 'control' field)")

    restored = pymongo.MongoClient(cluster.connection)["test"]
    result = restored.command("validate", "ts1", full=True)
    assert result["valid"], f"validate failed for ts1: {result}"
    assert restored["ts1"].count_documents({}) == before

    n = testinfra.get_host("docker://" + cluster.pbm_cli)
    logs = n.check_output("pbm logs -sD -t0")
    skips = logs.count("skipping update to missing time-series bucket")
    Cluster.log(f"Restore skipped {skips} updates to missing time-series buckets")
    assert skips > 0, "Timeseries skips not detected"
