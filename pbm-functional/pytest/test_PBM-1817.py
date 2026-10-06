import os
import time
from datetime import datetime, timezone

import pymongo
import pytest
from cluster import Cluster

# shop.orders, sharded by customer. legacy_1 is created in the snapshot and dropped during PITR.
ORDERS_INDEXES = {
    "status_1": {"key": [("status", 1)]},
    "customer_1": {"key": [("customer", 1)]},
    "customer_1_orderId_1": {"key": [("customer", 1), ("orderId", 1)], "unique": True},
}

# shop.invoices, sharded by account. account_1 is the shard key index.
# A draft index is created and dropped during PITR.
INVOICES_INDEXES = {
    "account_1": {"key": [("account", 1)]},
    "dueAt_1": {"key": [("dueAt", 1)], "expireAfterSeconds": 172800, "hidden": True},
    "account_1_amount_-1": {"key": [("account", 1), ("amount", -1)]},
    "issuedAt_1": {"key": [("issuedAt", 1)], "expireAfterSeconds": 7200, "hidden": True},
}

# audit.events, unsharded database. draft_1 is created in the snapshot and dropped during PITR.
EVENTS_INDEXES = {
    "user_1": {"key": [("user", 1)]},
    "action_1": {"key": [("action", 1)], "sparse": True},
}

# audit.traces, unsharded database.
TRACES_INDEXES = {
    "ts_1": {"key": [("ts", 1)], "expireAfterSeconds": 172800, "hidden": True},
    "traceId_1": {"key": [("traceId", 1)], "unique": True},
}


@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {
            "_id": "rscfg",
            "members": [{"host": "rscfg01"}, {"host": "rscfg02"}, {"host": "rscfg03"}],
        },
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}]},
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
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


def _docs(prefix, count, phase):
    now = datetime.now(timezone.utc)
    return [
        {
            "_id": f"{prefix}-{i}",
            "phase": phase,
            "customer": i,
            "status": "open",
            "legacy": i,
            "orderId": f"{prefix}-{i}",
            "account": i,
            "amount": i,
            "dueAt": now,
            "issuedAt": now,
            "draft": i,
            "user": f"user-{i}",
            "action": "login",
            "ts": now,
            "traceId": f"{prefix}-{i}",
        }
        for i in range(count)
    ]


def _assert_indexes(coll, expected, where):
    info = coll.index_information()
    got = set(info)
    want = set(expected) | {"_id_"}
    assert got == want, f"{where}: indexes {sorted(got)} != {sorted(want)}"
    for name, opts in expected.items():
        spec = info[name]
        for key, value in opts.items():
            actual = spec.get(key)
            if key == "expireAfterSeconds" and actual is not None:
                actual = int(actual)
            assert actual == value, (
                f"{where}: index {name} option {key} is {actual}, expected {value}"
            )


def _collmod(db, collection, name, **options):
    db.command("collMod", collection, index={"name": name, **options})


def _assert_collection(coll, count, pitr_count, indexes, where):
    assert coll.count_documents({}) == count, where
    assert coll.count_documents({"phase": "pitr"}) == pitr_count, where
    _assert_indexes(coll, indexes, where)


@pytest.mark.timeout(900, func_only=True)
def test_physical_pitr_index_ddl_sharded_PBM_T378(start_cluster, cluster):
    """
    Verify a physical PITR restore on a sharded cluster replays index DDL.

    shop is sharded and holds orders and invoices. audit is not sharded and
    holds events and traces. Each collection has its own indexes. Creates,
    drops, and collMod changes from the PITR window are visible through mongos.
    """
    client = pymongo.MongoClient(cluster.connection)
    client.admin.command("enableSharding", "shop")
    client.admin.command("shardCollection", "shop.orders", key={"customer": 1})
    client.admin.command("shardCollection", "shop.invoices", key={"account": 1})

    orders = client["shop"]["orders"]
    invoices = client["shop"]["invoices"]
    events = client["audit"]["events"]
    traces = client["audit"]["traces"]
    orders.insert_many(_docs("base-orders", 20, "base"))
    invoices.insert_many(_docs("base-invoices", 20, "base"))
    events.insert_many(_docs("base-events", 5, "base"))
    traces.insert_many(_docs("base-traces", 5, "base"))

    orders.create_index([("status", 1)], name="status_1")
    orders.create_index([("legacy", 1)], name="legacy_1")
    invoices.create_index([("dueAt", 1)], name="dueAt_1", expireAfterSeconds=86400)
    events.create_index([("user", 1)], name="user_1")
    events.create_index([("draft", 1)], name="draft_1")
    traces.create_index([("ts", 1)], name="ts_1", expireAfterSeconds=86400)

    # PITR starts only after a full backup exists. The snapshot must be taken
    # before the index DDL below, so those commands land in the PITR oplog.
    backup = cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    orders.create_index([("customer", 1)], name="customer_1")
    # A unique index on a sharded collection has to be prefixed with the shard key.
    orders.create_index([("customer", 1), ("orderId", 1)], name="customer_1_orderId_1", unique=True)
    orders.drop_index("legacy_1")

    invoices.create_index([("draft", 1)], name="draft_1")
    invoices.drop_index("draft_1")
    invoices.create_index([("account", 1), ("amount", -1)], name="account_1_amount_-1")
    invoices.create_index([("issuedAt", 1)], name="issuedAt_1", expireAfterSeconds=3600)
    _collmod(
        client["shop"],
        "invoices",
        "dueAt_1",
        expireAfterSeconds=INVOICES_INDEXES["dueAt_1"]["expireAfterSeconds"],
        hidden=True,
    )
    _collmod(
        client["shop"],
        "invoices",
        "issuedAt_1",
        expireAfterSeconds=INVOICES_INDEXES["issuedAt_1"]["expireAfterSeconds"],
        hidden=True,
    )

    events.drop_index("draft_1")
    events.create_index([("action", 1)], name="action_1", sparse=True)

    _collmod(
        client["audit"],
        "traces",
        "ts_1",
        expireAfterSeconds=TRACES_INDEXES["ts_1"]["expireAfterSeconds"],
        hidden=True,
    )
    traces.create_index([("traceId", 1)], name="traceId_1", unique=True)

    orders.insert_many(_docs("pitr-orders", 10, "pitr"))
    invoices.insert_many(_docs("pitr-invoices", 10, "pitr"))
    events.insert_many(_docs("pitr-events", 5, "pitr"))
    traces.insert_many(_docs("pitr-traces", 5, "pitr"))
    _assert_collection(orders, 30, 10, ORDERS_INDEXES, "orders before restore")
    _assert_collection(invoices, 30, 10, INVOICES_INDEXES, "invoices before restore")
    _assert_collection(events, 10, 5, EVENTS_INDEXES, "events before restore")
    _assert_collection(traces, 10, 5, TRACES_INDEXES, "traces before restore")

    time.sleep(2)
    pitr = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(2)
    cluster.disable_pitr(pitr)
    cluster.make_restore(
        f"--time={pitr} --base-snapshot={backup}",
        restart_cluster=True,
        check_pbm_status=True,
        timeout=600,
    )

    client = pymongo.MongoClient(cluster.connection)
    orders = client["shop"]["orders"]
    invoices = client["shop"]["invoices"]
    events = client["audit"]["events"]
    traces = client["audit"]["traces"]
    assert client["shop"].command("collstats", "orders").get("sharded") is True
    assert client["shop"].command("collstats", "invoices").get("sharded") is True
    assert client["audit"].command("collstats", "events").get("sharded", False) is False
    assert client["audit"].command("collstats", "traces").get("sharded", False) is False
    _assert_collection(orders, 30, 10, ORDERS_INDEXES, "orders")
    _assert_collection(invoices, 30, 10, INVOICES_INDEXES, "invoices")
    _assert_collection(events, 10, 5, EVENTS_INDEXES, "events")
    _assert_collection(traces, 10, 5, TRACES_INDEXES, "traces")

    with pytest.raises(pymongo.errors.DuplicateKeyError):
        orders.insert_one({"_id": "dup-order", "customer": 0, "orderId": "base-orders-0"})
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        traces.insert_one({"_id": "dup-trace", "traceId": "base-traces-0"})
    Cluster.log("Finished successfully")
