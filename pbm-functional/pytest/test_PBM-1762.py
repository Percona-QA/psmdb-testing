import os
import time
from datetime import datetime, timezone

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


@pytest.mark.timeout(600, func_only=True)
def test_pitr_rename_and_timeseries_out_PBM_T372(start_cluster, cluster):
    """
    PBM-1762: PITR restore fails with IllegalOperation because PBM
    attempts to create indexes on a non-existent collection after a TS
    collection was renamed. MongoDB does not allow user-performed renames
    of TS collections, but it can still rename the underlying buckets.
    $out does that for a temporary bucket collection during aggregation.
    """
    client = pymongo.MongoClient(cluster.connection)
    client.drop_database("test")
    ts_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    client["test"]["a"].insert_one({"_id": 1, "x": 1})
    client["test"]["src"].insert_one({"_id": 1, "t": ts_time, "m": 1})

    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    client["test"]["a"].create_index("x", name="x")
    client["test"]["a"].rename("b")
    list(client["test"]["src"].aggregate([
        {
            "$out": {
                "db": "test",
                "coll": "ts",
                "timeseries": {
                    "timeField": "t",
                    "metaField": "m",
                },
            }
        }
    ]))

    time.sleep(5)
    pitr = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr(pitr)

    client.drop_database("test")
    cluster.make_restore("--time=" + pitr, check_pbm_status=True)

    client = pymongo.MongoClient(cluster.connection)
    db = client["test"]
    names = db.list_collection_names()
    assert "a" not in names
    assert "b" in names
    assert "src" in names
    assert "ts" in names

    assert db["b"].find_one({"_id": 1}) == {"_id": 1, "x": 1}
    indexes = db["b"].index_information()
    assert "x" in indexes

    src = db["src"].find_one({"_id": 1})
    assert src is not None
    assert src["m"] == 1

    ts_info = next(db.list_collections(filter={"name": "ts"}), None)
    assert ts_info is not None
    ts_opts = ts_info.get("options", {}).get("timeseries", {})
    assert ts_opts.get("timeField") == "t"
    assert ts_opts.get("metaField") == "m"
    assert db["ts"].count_documents({}) == 1
    ts_doc = db["ts"].find_one()
    assert ts_doc["m"] == 1
    Cluster.log("Finished successfully")
