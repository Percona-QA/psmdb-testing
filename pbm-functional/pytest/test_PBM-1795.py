import time

import pymongo
import pytest
from cluster import Cluster


@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(
        config,
        mongod_extra_args=(
            " --setParameter=logicalSessionRefreshMillis=10000"
            " --setParameter=shutdownTimeoutMillisForSignaledShutdown=300"
            " --oplogSize=1024"
        ),
    )


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy(cleanup_backups=True)
        cluster.create()
        cluster.setup_pbm()
        # A restore that hangs never reaches delete-pitr, so the next run can
        # resync that chunk and treat it as the one under test.
        _purge_previous_backups(cluster)
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


def _insert_bulk(connection, doc_bytes, doc_count):
    client = pymongo.MongoClient(connection, w="majority")
    try:
        payload = b"x" * doc_bytes
        coll = client["test"]["bulk"]
        for start in range(0, doc_count, 100):
            coll.insert_many(
                [{"_id": i, "pad": payload} for i in range(start, min(start + 100, doc_count))],
                ordered=False,
            )
    finally:
        client.close()


def _purge_previous_backups(cluster):
    for cmd in ("delete-pitr --all --force --yes --wait", "delete-backup --older-than=0d --force --yes"):
        result = cluster.exec_pbm_cli(cmd)
        Cluster.log(result.stdout + result.stderr)
    deadline = time.time() + 60
    while cluster.get_status().get("running") and time.time() < deadline:
        time.sleep(1)


def _pitr_chunks(connection):
    client = pymongo.MongoClient(connection)
    try:
        return list(client["admin"]["pbmPITRChunks"].find().sort("start_ts", pymongo.ASCENDING))
    finally:
        client.close()


def _backup_last_write(connection, name):
    client = pymongo.MongoClient(connection)
    try:
        meta = client["admin"]["pbmBackups"].find_one({"name": name})
        assert meta and meta.get("last_write_ts"), f"Backup {name} has no last_write_ts"
        return meta["last_write_ts"]
    finally:
        client.close()


def _wait_for_large_chunk(cluster, last_write, min_chunk_bytes, chunk_wait_sec):
    """Wait until the chunk that covers this snapshot is large enough to trip the downloader"""
    target = last_write.time + 1
    deadline = time.time() + chunk_wait_sec
    last = []
    while time.time() < deadline:
        last = _pitr_chunks(cluster.connection)
        large = [
            c for c in last
            if c.get("size", 0) >= min_chunk_bytes
            and c["start_ts"].time <= target < c["end_ts"].time
        ]
        if large:
            chunk = large[0]
            Cluster.log(
                f"PITR chunk {chunk['fname']} size={chunk['size']} "
                f"start={chunk['start_ts']} end={chunk['end_ts']}"
            )
            return chunk
        time.sleep(5)
    assert False, (
        f"No PITR chunk covering {target} reached {min_chunk_bytes} bytes within {chunk_wait_sec}s, "
        f"saw {[(c.get('start_ts'), c.get('end_ts'), c.get('size')) for c in last]}. "
        "The restore would not exercise the stuck downloader"
    )


@pytest.mark.jenkins
@pytest.mark.timeout(1200, func_only=True)
def test_physical_pitr_restore_to_range_start_PBM_T383(start_cluster, cluster):
    """
    Physical PITR restore must finish when the target is the beginning of a
    large oplog chunk on storage that downloads in parallel chunks (S3/minio).

    Replaying only the beginning of that chunk used to leave the shared
    download buffer occupied, so every node then blocked forever reading
    the restore heartbeat file. The test fails with a restore timeout on
    previous versions.
    """
    # A PITR chunk of about this size is enough for replay to stop while the
    # shared multi-chunk downloader still holds its buffer (PBM-1795).
    min_chunk_bytes = 250 * 1024 * 1024
    doc_bytes = 32 * 1024
    doc_count = 9000  # ~281MiB of oplog, above min_chunk_bytes
    # Span must outlast the insert loop so the whole payload lands in one chunk.
    oplog_span_min = 2
    chunk_wait_sec = oplog_span_min * 60 + 120
    restore_timeout_sec = 480

    client = pymongo.MongoClient(cluster.connection, w="majority")
    client["test"]["base"].insert_one({"_id": "base"})
    client.close()

    backup = cluster.make_backup("physical")
    # The chunk starts at the snapshot's last write, which PBM refuses as a
    # restore target. The next second is the beginning of the restorable range
    # and still the prefix of this chunk, before the bulk of the oplog.
    last_write = _backup_last_write(cluster.connection, backup)
    cluster.enable_pitr(pitr_extra_args=f"--set pitr.oplogSpanMin={oplog_span_min}")

    started = time.time()
    _insert_bulk(cluster.connection, doc_bytes, doc_count)
    Cluster.log(f"Inserted {doc_count} documents in {time.time() - started:.0f}s")

    _wait_for_large_chunk(cluster, last_write, min_chunk_bytes, chunk_wait_sec)
    target = last_write.time + 1
    restore_time = f"{target},0"
    Cluster.log("Restore target is the beginning of the PITR range: " + restore_time)

    cluster.disable_pitr()
    cluster.make_restore(
        "--time=" + restore_time,
        restart_cluster=True,
        check_pbm_status=True,
        timeout=restore_timeout_sec,
    )

    client = pymongo.MongoClient(cluster.connection)
    try:
        assert client["test"]["base"].count_documents({}) == 1, "Snapshot document is missing after restore"
        restored_bulk = client["test"]["bulk"].count_documents({})
        assert restored_bulk == 0, (
            f"Restore to the start of the PITR range applied {restored_bulk} documents "
            "written after that point"
        )
    finally:
        client.close()
