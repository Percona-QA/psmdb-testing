import concurrent.futures
import os
import random
import threading
import time
from datetime import datetime, timedelta

import pytest
import pymongo

from cluster import Cluster

NORMAL_DOC_COUNT = 50
NORMAL_ID_START = 1000

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
    now = datetime.utcnow()
    docs = [
        {
            "_id": i,
            "instanceId": f"instance-{i}",
            "lastBeat": now,
            "n": 0,
            "validTill": now + timedelta(seconds=5),
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

def _heartbeat_writer(connection, doc_ids, duration, stop_event):
    client = pymongo.MongoClient(connection)
    coll = client["ttl_pitr_db"]["heartbeats"]
    n = 0
    deadline = time.time() + duration
    while time.time() < deadline and not stop_event.is_set():
        doc_id = random.choice(doc_ids)
        n += 1
        now = datetime.utcnow()
        try:
            coll.update_one(
                {"_id": doc_id},
                {"$set": {"lastBeat": now, "n": n, "validTill": now + timedelta(seconds=5)}},
            )
        except pymongo.errors.PyMongoError:
            pass

@pytest.mark.timeout(3600, func_only=True)
def test_physical_pitr_restore_with_ttl_unique_index_PBM_1779(start_cluster, cluster):
    """

    """

    doc_ids = _seed_heartbeats(cluster.connection)
    _seed_normal_docs(cluster.connection)

    cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    stop_event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        writers = [
            executor.submit(_heartbeat_writer, cluster.connection, doc_ids, 240, stop_event)
            for _ in range(20)
        ]
        try:
            for w in writers:
                w.result()
        finally:
            stop_event.set()
    Cluster.log("Update workload finished")

    pymongo.MongoClient(cluster.connection)["ttl_pitr_db"]["heartbeats"].update_many(
        {"_id": {"$gte": NORMAL_ID_START}}, {"$set": {"version": 1}}
    )

    # Give the TTL monitor on the live cluster a beat to catch up so PITR
    # captures TTL-driven deletes too, not just the heartbeat updates.
    time.sleep(10)

    pitr_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(pitr_time)
    time.sleep(10)

    restore_arg = f"--time={pitr_time}"
    try:
        cluster.make_restore(restore_arg, restart_cluster=True, check_pbm_status=True)
    except AssertionError as e:
        dup_key_lines = [l for l in str(e).splitlines() if "E11000" in l or "duplicate key" in l.lower()]
        if dup_key_lines:
            pytest.fail(f"Restore failed with a duplicate-key error during oplog replay (TTL deleted docs mid-replay)\n{dup_key_lines[0]}", pytrace=False)
        raise

    client = pymongo.MongoClient(cluster.connection)
    coll = client["ttl_pitr_db"]["heartbeats"]

    null_instance_docs = list(coll.find({"instanceId": None}))
    assert not null_instance_docs, f"Found {len(null_instance_docs)} document(s) with a null instanceId"

    instance_ids = [d["instanceId"] for d in coll.find({}, {"instanceId": 1})]
    assert len(instance_ids) == len(set(instance_ids)), "Duplicate instanceId values found in destination"

    normal_filter = {"_id": {"$gte": NORMAL_ID_START}}
    assert coll.count_documents(normal_filter) == NORMAL_DOC_COUNT, "Non-expiring documents were lost during the restore"
    assert coll.count_documents({**normal_filter, "version": 1}) == NORMAL_DOC_COUNT, "Updates made to normal documents during the PITR window were not replayed"

    deadline = time.time() + 180
    while coll.count_documents({"_id": {"$lt": NORMAL_ID_START}}) and time.time() < deadline:
        time.sleep(5)
    remaining = coll.count_documents({"_id": {"$lt": NORMAL_ID_START}})
    assert remaining == 0, f"{remaining} expired heartbeat document(s) still present"
    assert coll.count_documents(normal_filter) == NORMAL_DOC_COUNT, "TTL cleanup after restore removed non-expiring documents"
