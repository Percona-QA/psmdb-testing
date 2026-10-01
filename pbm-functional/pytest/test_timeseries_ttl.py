import concurrent.futures
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

import pymongo
import pytest
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

@pytest.mark.timeout(900, func_only=True)
def test_logical_pitr_expiring_timeseries_ttl_off_restore(start_cluster, cluster):
    client = pymongo.MongoClient(cluster.connection)
    client.admin.command({"setParameter": 1, "ttlMonitorSleepSecs": 1})
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

    Cluster.log("Generating expiring timeseries data for 60 seconds")
    time.sleep(60)
    stop_event.set()
    future.result()
    executor.shutdown()
    cluster.disable_pitr()
    time.sleep(6)

    pitr_end = cluster.get_last_pitr_chunk_end()
    pitr_time = datetime.fromtimestamp(pitr_end, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    client.admin.command({"setParameter": 1, "ttlMonitorEnabled": False})
    time.sleep(3)
    before = client["test"]["ts1"].count_documents({})

    client["test"].drop_collection("ts1")
    cluster.make_restore("--time=" + pitr_time, check_pbm_status=True)

    restored = pymongo.MongoClient(cluster.connection)["test"]
    result = restored.command("validate", "ts1", full=True)
    assert result["valid"], f"validate failed for ts1: {result}"
    assert restored["ts1"].count_documents({}) == before
    Cluster.log("Finished successfully")
