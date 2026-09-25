import concurrent.futures
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

import pymongo
import pytest
from cluster import Cluster

NORMAL_DOC_COUNT = 50
NORMAL_ID_START = 1000
WRITER_THREADS = 20
TOTAL_UPDATES = 480_000

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def _seed_heartbeats(connection):
    client = pymongo.MongoClient(connection)
    coll = client["ttl_pitr_db"]["heartbeats"]
    coll.create_index("instanceId", unique=True)
    coll.create_index("validTill", expireAfterSeconds=0)
    now = datetime.now(timezone.utc)
    docs = [
        {
            "_id": i,
            "instanceId": f"instance-{i}",
            "lastBeat": now,
            "n": 0,
            "validTill": now + timedelta(hours=1),
        }
        for i in range(100)
    ]
    coll.insert_many(docs)
    return [d["_id"] for d in docs]

def _seed_normal_docs(connection):
    client = pymongo.MongoClient(connection)
    coll = client["ttl_pitr_db"]["heartbeats"]
    docs = [
        {"_id": NORMAL_ID_START + i, "instanceId": f"normal-{i}", "version": 0}
        for i in range(NORMAL_DOC_COUNT)
    ]
    coll.insert_many(docs)

def _heartbeat_writer(connection, doc_ids, total_updates, stop_event):
    client = pymongo.MongoClient(connection)
    coll = client["ttl_pitr_db"]["heartbeats"]
    n = 0
    applied = 0
    while n < total_updates and not stop_event.is_set():
        now = datetime.now(timezone.utc)
        batch = []
        for _ in range(min(500, total_updates - n)):
            n += 1
            batch.append(
                pymongo.UpdateOne(
                    {"_id": random.choice(doc_ids)},
                    {"$set": {"lastBeat": now, "n": n, "validTill": now + timedelta(seconds=5)}},
                )
            )
        try:
            applied += coll.bulk_write(batch, ordered=False).matched_count
        except pymongo.errors.BulkWriteError as e:
            applied += e.details.get("nMatched", 0)
        except pymongo.errors.PyMongoError:
            pass
    return applied

@pytest.mark.timeout(900, func_only=True)
def test_physical_pitr_restore_with_ttl_unique_index_PBM_T373(start_cluster, cluster):
    """
        Verify that TTL cleanup does not interfere with a physical PITR restore,
        normal updates are still replayed, and TTL cleanup works again afterwards
    """

    doc_ids = _seed_heartbeats(cluster.connection)
    _seed_normal_docs(cluster.connection)

    cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    stop_event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=WRITER_THREADS) as executor:
        writers = [
            executor.submit(_heartbeat_writer, cluster.connection, doc_ids, TOTAL_UPDATES // WRITER_THREADS, stop_event)
            for _ in range(WRITER_THREADS)
        ]
        try:
            updates_applied = sum(w.result() for w in writers)
        finally:
            stop_event.set()
    assert updates_applied >= TOTAL_UPDATES * 0.9, (
        f"Setup: only {updates_applied}/{TOTAL_UPDATES} heartbeat updates matched a document -- "
        "the workload did not generate the oplog volume this test needs"
    )

    pymongo.MongoClient(cluster.connection)["ttl_pitr_db"]["heartbeats"].update_many(
        {"_id": {"$gte": NORMAL_ID_START}}, {"$set": {"version": 1}}
    )

    time.sleep(2)

    pitr_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(pitr_time)

    restore_arg = f"--time={pitr_time}"
    try:
        cluster.make_restore(restore_arg, restart_cluster=True, check_pbm_status=True, timeout=600)
    except AssertionError as e:
        dup_key_lines = [l for l in str(e).splitlines() if "E11000" in l or "duplicate key" in l.lower()]
        if dup_key_lines:
            pytest.fail(f"Restore failed with a duplicate-key error during oplog replay (TTL deleted docs mid-replay)\n{dup_key_lines[0]}", pytrace=False)
        raise

    client = pymongo.MongoClient(cluster.connection)
    coll = client["ttl_pitr_db"]["heartbeats"]

    null_instance_docs = list(coll.find({"instanceId": None}))
    assert not null_instance_docs, f"Found {len(null_instance_docs)} document(s) with a null instanceId"
    indexes = coll.index_information()
    assert indexes.get("instanceId_1", {}).get("unique") is True, "Unique instanceId index was not restored"
    assert indexes.get("validTill_1", {}).get("expireAfterSeconds") == 0, "TTL index was not restored"

    instance_ids = [d["instanceId"] for d in coll.find({}, {"instanceId": 1})]
    assert len(instance_ids) == len(set(instance_ids)), "Duplicate instanceId values found in destination"

    normal_filter = {"_id": {"$gte": NORMAL_ID_START}}
    assert coll.count_documents(normal_filter) == NORMAL_DOC_COUNT, "Non-expiring documents were lost during the restore"
    assert coll.count_documents({**normal_filter, "version": 1}) == NORMAL_DOC_COUNT, "Updates made to normal documents during the PITR window were not replayed"

    ttl_wait_start = time.time()
    deadline = ttl_wait_start + 180
    while coll.count_documents({"_id": {"$lt": NORMAL_ID_START}}) and time.time() < deadline:
        time.sleep(1)
    remaining = coll.count_documents({"_id": {"$lt": NORMAL_ID_START}})
    assert remaining == 0, f"{remaining} expired heartbeat document(s) still present"
    assert coll.count_documents(normal_filter) == NORMAL_DOC_COUNT, "TTL cleanup after restore removed non-expiring documents"
