import pytest
import pymongo
import time

from cluster import Cluster
from data_integrity_check import compare_data

# Filler shape -- large enough that with cloneNumParallelCollections=1 and
# cloneNumReadWorkers=1, bulk-copy of all fillers takes long enough for the
# test's writes to land between PCSM's startTS capture and the moment the
# victim collection is actually copied. Keep the fillers > 1 GiB total so even
# fast local docker hosts don't blow through the clone in <2 s
NUM_FILLER_COLLECTIONS = 3
FILLER_DOCS_PER_COLL = 3_000_000
FILLER_PAYLOAD_BYTES = 512

def _seed_fillers_and_victim(src, db, victim, victim_doc):
    for i in range(NUM_FILLER_COLLECTIONS):
        chunk = 50_000
        for offset in range(0, FILLER_DOCS_PER_COLL, chunk):
            src[db][f"filler_{i}"].insert_many(
                [{"_id": j, "payload": "x" * FILLER_PAYLOAD_BYTES}
                 for j in range(offset, min(offset + chunk, FILLER_DOCS_PER_COLL))],
                ordered=False,
            )
    src[db][victim].insert_one(victim_doc)

def _wait_for_clone_in_progress(csync, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        resp = csync.status()
        if resp.get("success"):
            data = resp.get("data") or {}
            initial_sync = data.get("initialSync") or {}
            cloned = initial_sync.get("clonedSizeBytes") or 0
            estimated = initial_sync.get("estimatedCloneSizeBytes") or 0
            clone_completed = bool(initial_sync.get("cloneCompleted"))
            if cloned > 0 and not clone_completed:
                return cloned, estimated
            if clone_completed:
                raise AssertionError(
                    "Clone already completed before test could issue writes -- "
                    f"window too narrow. cloned={cloned} estimated={estimated}. "
                    "Increase FILLER_DOCS_PER_COLL or NUM_FILLER_COLLECTIONS")
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for PCSM clone to enter bulk-copy phase")


@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(900, func_only=True)
def test_csync_PML_T94(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to validate handling of a path-collision $set issued during the
    clone phase. While PCSM is mid-clone, the source performs
    $set arr.0.lastScrapeAt = "x" (parent is still an object) followed by
    $set arr.0 = null. PCSM must reconcile these change-stream events with
    the cloned snapshot (where arr.0 is already null) so target.arr matches
    source.arr == [None] after finalize.
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db = "csync_path_collision_during_clone"
    victim = "victim"
    if src_cluster.is_sharded:
        src.admin.command("enableSharding", db)

    # arr.0 must be populated object: MongoDB change stream reports
    # UpdatedFields at most-specific path that covers the diff. With
    # sibling fields that don't change, the path stays at the leaf
    # (arr.0.lastScrapeAt) -- which is what is required to trigger the bug
    _seed_fillers_and_victim(
        src, db, victim, {"_id": 1, "arr": [{"sibling_a": 1, "sibling_b": "stable", "sibling_c": [1, 2, 3]}]})

    # Advance source cluster time past the seed inserts before PCSM
    # captures startTS. Otherwise PCSM's startTS shares a clusterTime with
    # the seed insert (startAtOperationTime is inclusive), the seed insert
    # is replayed during change replication, and the (pre-mutation)
    # populated arr.0 is restored onto the cloned target -- silently
    # masking the path-collision because the deferred $set runs against an
    # object instead of null
    src[db]["_fence"].insert_one({"_id": "pre-clone-fence", "ts": time.time()})
    time.sleep(1)

    assert csync.start(raw_args={
        "cloneNumParallelCollections": 1,
        "cloneNumReadWorkers": 1,
        "cloneNumInsertWorkers": 1}), "Failed to start csync service"
    cloned, estimated = _wait_for_clone_in_progress(csync)
    Cluster.log(
        f"Clone in progress when issuing mutations: "
        f"{cloned}/{estimated} bytes ({(100*cloned)/max(estimated,1):.1f}%)")

    # Sanity guard -- the victim must NOT yet have been cloned to the target when we issue the mutations
    pre_count = dst[db][victim].count_documents({})
    assert pre_count == 0, (
        f"Victim was already cloned to target ({pre_count} docs) before "
        "mutations -- window too narrow; bump filler size.")
    src[db][victim].update_one({"_id": 1}, {"$set": {"arr.0.lastScrapeAt": "x"}})
    src[db][victim].update_one({"_id": 1}, {"$set": {"arr.0": None}})

    assert csync.wait_for_repl_stage(timeout=600), "Failed to start replication stage"
    # Drop the multi-GB filler collections so simplify data integrity check
    for i in range(NUM_FILLER_COLLECTIONS):
        src[db][f"filler_{i}"].drop()
    assert csync.wait_for_zero_lag(timeout=600), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    src_doc = src[db][victim].find_one({"_id": 1})
    dst_doc = dst[db][victim].find_one({"_id": 1})
    assert src_doc["arr"] == [None], \
        f"Sanity: source arr should be [None], got {src_doc['arr']}"
    assert dst_doc["arr"] == src_doc["arr"], (
        f"Path-collision corruption: source arr={src_doc['arr']}, "
        f"target arr={dst_doc['arr']}. PCSM likely failed to apply "
        f"$set arr.0.lastScrapeAt against the cloned snapshot whose "
        f"arr.0 was already null.")
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
