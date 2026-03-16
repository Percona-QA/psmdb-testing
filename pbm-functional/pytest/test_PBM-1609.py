import threading
import time

import pymongo
import pytest

from cluster import Cluster

@pytest.fixture(scope="function")
def config(cluster_configs):
    return cluster_configs

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

def insert_documents(connection, stop_event):
    """Continuously insert documents to keep the oplog busy."""
    client = pymongo.MongoClient(connection)
    try:
        i = 0
        while not stop_event.is_set():
            client["test"]["pitr_load"].insert_one({"i": i})
            i += 1
            time.sleep(0.05)
    finally:
        client.close()

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_pitr_stopped_during_restore_PBM_T317(start_cluster, cluster):
    """
    Test that the oplog slicer is stopped and does not create any chunks during the restore phase.
    """
    client = pymongo.MongoClient(cluster.connection)
    try:
        client["test"]["pitr_load"].insert_many([{"i": i} for i in range(10)])
    finally:
        client.close()

    backup = cluster.make_backup("logical")
    Cluster.log(f"Backup completed: {backup}")

    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    stop_event = threading.Event()
    insert_documents_thread = threading.Thread(target=insert_documents, args=(cluster.connection, stop_event))
    insert_documents_thread.start()

    # Wait for at least 2 PITR chunks to be created before restoring
    timeout = time.time() + 120
    try:
        while True:
            pitr_logs = cluster.exec_pbm_cli("logs -sD -t0 -e pitr")
            chunks_created = [line for line in pitr_logs.stdout.splitlines() if "created chunk" in line]
            if len(chunks_created) >= 2:
                break
            assert time.time() < timeout, "Timed out waiting for PITR chunks to be created"
            time.sleep(2)

        cluster.make_restore(backup)
    finally:
        stop_event.set()
        insert_documents_thread.join()

    pbm_logs = cluster.exec_pbm_cli("logs -sD -t0")
    lines = pbm_logs.stdout.splitlines()

    recovery_started = None
    for idx, line in enumerate(lines):
        if "recovery started" in line:
            recovery_started = idx
            break

    assert recovery_started is not None, "'recovery started' was not found in PBM logs"

    chunks_after = [line for line in lines[recovery_started + 1:] if "created chunk" in line]
    assert not chunks_after, "PITR chunks were created after restore started:\n" + "\n".join(chunks_after)
