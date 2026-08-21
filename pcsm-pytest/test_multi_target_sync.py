import time

import pytest
import pymongo
import threading
from datetime import datetime, timezone

from cluster import Cluster
from clustersync import Clustersync
from data_integrity_check import compare_data

DB_A = "db_0"
DB_B = "db_1"
COLL = "docs"
INITIAL_DOC_COUNT = 100_000

def _make_config(rs_name, host):
    return {"_id": rs_name, "members": [{"host": host}]}

def _make_sharded_config(mongos, cfg_rs, cfg_host, shard_rs_a, shard_host_a, shard_rs_b, shard_host_b):
    return {
        "mongos": mongos,
        "configserver": _make_config(cfg_rs, cfg_host),
        "shards": [
            _make_config(shard_rs_a, shard_host_a),
            _make_config(shard_rs_b, shard_host_b),
        ],
    }

def _topology_configs(topology):
    """
    Config sets for a 1-source/2-target topology, keyed by "src"/"dst_a"/"dst_b".
    Hostnames/rs ids are unique across all three clusters within a topology.
    """
    if topology == "replicaset":
        return {
            "src": _make_config("rs1", "rs101"),
            "dst_a": _make_config("rs2", "rs201"),
            "dst_b": _make_config("rs3", "rs301"),
        }
    elif topology == "sharded":
        return {
            "src": _make_sharded_config("mongos1", "rscfg1", "rscfg101", "rs1", "rs101", "rs2", "rs201"),
            "dst_a": _make_sharded_config("mongos2", "rscfg2", "rscfg201", "rs3", "rs301", "rs4", "rs401"),
            "dst_b": _make_sharded_config("mongos3", "rscfg3", "rscfg301", "rs5", "rs501", "rs6", "rs601"),
        }
    raise ValueError(f"Unknown topology: {topology}")

def _mongod_extra_args(config):
    # Sharded clusters need frequent no-op writes for change-stream resumability
    return "--setParameter periodicNoopIntervalSecs=1" if "mongos" in config else ""

@pytest.fixture(scope="function")
def topology_configs(request):
    return _topology_configs(request.param)

@pytest.fixture(scope="function")
def src_cluster(topology_configs):
    config = topology_configs["src"]
    return Cluster(config, mongod_extra_args=_mongod_extra_args(config), mongo_image="mongodb-src/local")

@pytest.fixture(scope="function")
def dst_cluster_a(topology_configs):
    config = topology_configs["dst_a"]
    return Cluster(config, mongod_extra_args=_mongod_extra_args(config), mongo_image="mongodb-dst/local")

@pytest.fixture(scope="function")
def dst_cluster_b(topology_configs):
    config = topology_configs["dst_b"]
    return Cluster(config, mongod_extra_args=_mongod_extra_args(config), mongo_image="mongodb-dst/local")

@pytest.fixture(scope="function")
def csync_a(src_cluster, dst_cluster_a, request):
    csync_instance = Clustersync('csync-a', src_cluster.csync_connection, dst_cluster_a.csync_connection)
    request.addfinalizer(csync_instance.destroy)
    return csync_instance

@pytest.fixture(scope="function")
def csync_b(src_cluster, dst_cluster_b, request):
    csync_instance = Clustersync('csync-b', src_cluster.csync_connection, dst_cluster_b.csync_connection)
    request.addfinalizer(csync_instance.destroy)
    return csync_instance

@pytest.fixture(scope="function")
def start_clusters(src_cluster, dst_cluster_a, dst_cluster_b, csync_a, csync_b, request):
    try:
        src_cluster.destroy()
        dst_cluster_a.destroy()
        dst_cluster_b.destroy()
        csync_a.destroy()
        csync_b.destroy()
        create_threads = [
            threading.Thread(target=src_cluster.create),
            threading.Thread(target=dst_cluster_a.create),
            threading.Thread(target=dst_cluster_b.create),
        ]
        for thread in create_threads:
            thread.start()
        for thread in create_threads:
            thread.join()
        csync_a.create()
        csync_b.create()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            print(f"\n\ncsync-a Last 50 Logs:\n{csync_a.logs()}\n\n")
            print(f"\n\ncsync-b Last 50 Logs:\n{csync_b.logs()}\n\n")
        src_cluster.destroy()
        dst_cluster_a.destroy()
        dst_cluster_b.destroy()
        csync_a.destroy()
        csync_b.destroy()

def _insert_docs(connection, db_name, count, start_id=0, batch_size=10_000):
    client = pymongo.MongoClient(connection)
    coll = client[db_name][COLL]
    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        coll.insert_many([
            {
                "_id": start_id + i,
                "value": i,
                "uid": f"{db_name}-{start_id + i}",
                "tag": f"sample text number {i}",
                "created_at": datetime.now(timezone.utc),
            }
            for i in range(batch_start, batch_end)
        ])
    client.close()

def _create_indexes(connection, db_name):
    """
    Create a variety of index types on the collection before syncing starts,
    so the initial clone has to carry them over
    """
    client = pymongo.MongoClient(connection)
    coll = client[db_name][COLL]
    coll.create_index([("value", 1)])                                  # single-field
    coll.create_index([("value", 1), ("uid", -1)])                     # compound
    coll.create_index([("uid", 1)], unique=True)                       # unique
    coll.create_index([("tag", "text")])                               # text
    coll.create_index([("_id", "hashed")])                             # hashed
    coll.create_index([("created_at", 1)], expireAfterSeconds=86400)   # TTL
    client.close()

def _create_indexes_live(connection, db_name):
    """
    Create indexes while the sync is already running, exercising the DDL
    replication path (change stream) instead of the initial-clone path
    """
    client = pymongo.MongoClient(connection)
    coll = client[db_name][COLL]
    coll.create_index([("value", -1)], name="value_partial",
                       partialFilterExpression={"value": {"$gte": INITIAL_DOC_COUNT // 2}})  # partial
    coll.create_index([("tag", 1)], name="tag_sparse", sparse=True)                          # sparse
    client.close()

def _assert_only_own_subset_synced(src_cluster, dst_cluster, own_db, other_db, label):
    """
    Compare src -> dst with compare_data
    Also verify what's not supposed to be there
    """
    result, mismatches = compare_data(src_cluster, dst_cluster)
    if result:
        pytest.fail(f"{label}: expected {other_db} to be reported missing, but compare_data found no mismatches at all")
    unexpected = [(name, reason) for name, reason in mismatches if not name.startswith(other_db)]
    assert not unexpected, f"{label}: unexpected mismatch in its own subset ({own_db}): {unexpected}"
    assert any(name.startswith(other_db) for name, _ in mismatches), \
        f"{label}: {other_db} was not reported as missing by compare_data: {mismatches}"

@pytest.mark.parametrize("topology_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(3600, func_only=True)
def test_multi_target_disjoint_sync_PLM_T108(start_clusters, src_cluster, dst_cluster_a, dst_cluster_b, csync_a, csync_b):
    """
    Verify sync from one source to two different destinations
    """
    _insert_docs(src_cluster.connection, DB_A, INITIAL_DOC_COUNT)
    _insert_docs(src_cluster.connection, DB_B, INITIAL_DOC_COUNT)
    _create_indexes(src_cluster.connection, DB_A)
    _create_indexes(src_cluster.connection, DB_B)

    # Start both csync instances concurrently, each scoped to its own database
    start_results = {}

    def start(name, csync, db_name):
        start_results[name] = csync.start(raw_args={"includeNamespaces": [f"{db_name}.*"]})

    start_threads = [
        threading.Thread(target=start, args=("a", csync_a, DB_A)),
        threading.Thread(target=start, args=("b", csync_b, DB_B)),
    ]
    for thread in start_threads:
        thread.start()
    for thread in start_threads:
        thread.join()
    assert start_results.get("a"), "Failed to start csync-a"
    assert start_results.get("b"), "Failed to start csync-b"

    assert csync_a.wait_for_repl_stage(timeout=300), "csync-a failed to complete initial clone"
    assert csync_b.wait_for_repl_stage(timeout=300), "csync-b failed to complete initial clone"

    # Live writes while both syncs are running, to the correct source database each
    _insert_docs(src_cluster.connection, DB_A, 1, start_id=INITIAL_DOC_COUNT)
    _insert_docs(src_cluster.connection, DB_B, 1, start_id=INITIAL_DOC_COUNT)

    # Live index creation while both syncs are running
    _create_indexes_live(src_cluster.connection, DB_A)
    _create_indexes_live(src_cluster.connection, DB_B)

    assert csync_a.wait_for_zero_lag(), "csync-a failed to catch up on replication"
    assert csync_b.wait_for_zero_lag(), "csync-b failed to catch up on replication"

    assert csync_a.finalize(), "Failed to finalize csync-a"
    assert csync_b.finalize(), "Failed to finalize csync-b"

    _assert_only_own_subset_synced(src_cluster, dst_cluster_a, DB_A, DB_B, "Target A")
    _assert_only_own_subset_synced(src_cluster, dst_cluster_b, DB_B, DB_A, "Target B")

    csync_a_ok, csync_a_errors = csync_a.check_csync_errors()
    assert csync_a_ok, f"csync-a reported errors in logs: {csync_a_errors}"
    csync_b_ok, csync_b_errors = csync_b.check_csync_errors()
    assert csync_b_ok, f"csync-b reported errors in logs: {csync_b_errors}"
