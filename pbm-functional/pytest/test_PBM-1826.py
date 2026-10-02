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


@pytest.fixture(scope="package")
def sharded_config():
    return {"mongos": "mongos",
            "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
            "shards": [
                {"_id": "rs1", "members": [{"host": "rs101"}]},
                {"_id": "rs2", "members": [{"host": "rs201"}]}
            ]}


@pytest.fixture(scope="package")
def sharded_cluster(sharded_config):
    return Cluster(sharded_config)


@pytest.fixture(scope="function")
def start_sharded_cluster(sharded_cluster, request):
    try:
        sharded_cluster.destroy(cleanup_backups=True)
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        sharded_cluster.create()
        sharded_cluster.setup_pbm("/etc/pbm-fs.conf")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            sharded_cluster.get_logs()
        sharded_cluster.destroy(cleanup_backups=True)


def check_index_moved(db, old_names, new_name, index_name):
    names = db.list_collection_names()
    assert new_name in names, f"{new_name} is missing after restore"
    assert db[new_name].count_documents({}) == 10
    new_indexes = db[new_name].index_information()
    old_indexes = {old: list(db[old].index_information()) for old in old_names if old in names}
    assert index_name in new_indexes, \
        f"Index {index_name} is missing on {new_name}, indexes left on old namespaces: {old_indexes}"
    assert not old_indexes, \
        f"Old namespaces were recreated by restore: {old_indexes}"


@pytest.mark.timeout(600, func_only=True)
@pytest.mark.parametrize("backup_type", ["logical", "physical"])
def test_pitr_rename_collection_indexes_PBM_T374(start_cluster, cluster, backup_type):
    """
        Verify that indexes follow renamed collections during PITR oplog
         replay and old names are not recreated on a replicaset environment
    """

    client = pymongo.MongoClient(cluster.connection)
    db = client["test"]
    db["c1"].insert_many([{"_id": i, "a": i} for i in range(10)])
    db["c1"].create_index("a", name="a_idx")
    db["c3"].insert_many([{"_id": i, "c": i} for i in range(10)])
    db["c3"].create_index("c", name="c_idx")

    backup = cluster.make_backup(backup_type)
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    db["c2"].insert_many([{"_id": i, "b": i} for i in range(10)])
    db["c2"].create_index("b", name="b_idx")
    db["c4"].insert_many([{"_id": i, "d": i} for i in range(10)])
    db["c4"].create_index("d", name="d_idx")
    db["c1"].rename("c1b")
    db["c2"].rename("c2b")
    db["c3"].rename("c3a")
    db["c3a"].rename("c3b")
    db["c4"].rename("c4a")
    db["c4a"].rename("c4b")

    time.sleep(5)
    pitr = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(pitr)

    if backup_type == "logical":
        client.drop_database("test")
        cluster.make_restore("--time=" + pitr, check_pbm_status=True)
    else:
        cluster.make_restore("--time=" + pitr + " --base-snapshot=" + backup,
                             restart_cluster=True, check_pbm_status=True)

    db = pymongo.MongoClient(cluster.connection)["test"]

    check_index_moved(db, ["c1"], "c1b", "a_idx")
    check_index_moved(db, ["c2"], "c2b", "b_idx")
    check_index_moved(db, ["c3", "c3a"], "c3b", "c_idx")
    check_index_moved(db, ["c4", "c4a"], "c4b", "d_idx")


def check_index_moved_sharded(client, old_names, new_name, index_name, sharded):
    db = client["test"]
    names = db.list_collection_names()
    assert new_name in names, f"{new_name} is missing after restore"
    assert db[new_name].count_documents({}) == 100
    new_indexes = db[new_name].index_information()
    old_indexes = {old: list(db[old].index_information()) for old in old_names if old in names}
    assert index_name in new_indexes, \
        f"Index {index_name} is missing on {new_name}, indexes left on old namespaces: {old_indexes}"
    assert not old_indexes, \
        f"Old namespaces were recreated by restore: {old_indexes}"
    if sharded:
        assert "_id_hashed" in new_indexes, \
            f"Shard key index _id_hashed is missing on {new_name}, indexes left on old namespaces: {old_indexes}"

    # Sharding metadata must follow the rename too
    meta = client["config"]["collections"]
    for old in old_names:
        assert meta.find_one({"_id": f"test.{old}"}) is None, \
            f"Sharding metadata still exists for old namespace test.{old}"
    if sharded:
        assert meta.find_one({"_id": f"test.{new_name}"}) is not None, \
            f"Sharding metadata is missing for test.{new_name}"

    # Check each shard directly: a stray old-name collection could exist on one shard only
    SHARD_HOSTS = {"rs1": "rs101", "rs2": "rs201"}
    for rs, host in SHARD_HOSTS.items():
        shard_db = pymongo.MongoClient(f"mongodb://root:root@{host}:27017/")["test"]
        shard_names = shard_db.list_collection_names()
        stray = [old for old in old_names if old in shard_names]
        assert not stray, f"Old namespaces {stray} were recreated on shard {rs}"
        if new_name in shard_names:
            assert index_name in shard_db[new_name].index_information(), \
                f"Index {index_name} is missing on {new_name} on shard {rs}"
        else:
            assert not sharded, f"Sharded collection {new_name} is missing on shard {rs}"


@pytest.mark.timeout(900, func_only=True)
def test_pitr_rename_collection_indexes_sharded_PBM_T375(start_sharded_cluster, sharded_cluster):
    """
        Verify that indexes follow renamed collections during PITR oplog
        replay and old names are not recreated on a sharded environment
    """
    client = pymongo.MongoClient(sharded_cluster.connection)
    client.admin.command({"enableSharding": "test", "primaryShard": "rs1"})
    db = client["test"]

    for name in ["c1", "c4"]:
        client.admin.command("shardCollection", f"test.{name}", key={"_id": "hashed"})
    for name, field in [("c1", "a"), ("c2", "b"), ("c3", "c"), ("c4", "d")]:
        db[name].insert_many([{"_id": i, field: i} for i in range(100)])
    db["c1"].create_index("a", name="a_idx")
    db["c3"].create_index("c", name="c_idx")

    sharded_cluster.make_backup("logical")
    sharded_cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    db["c2"].create_index("b", name="b_idx")
    db["c4"].create_index("d", name="d_idx")
    db["c1"].rename("c1b")
    db["c2"].rename("c2b")
    db["c3"].rename("c3a")
    db["c3a"].rename("c3b")
    db["c4"].rename("c4a")
    db["c4a"].rename("c4b")

    time.sleep(5)
    pitr = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    time.sleep(5)
    sharded_cluster.disable_pitr(pitr)

    client.drop_database("test")
    sharded_cluster.make_restore("--time=" + pitr, check_pbm_status=True)

    client = pymongo.MongoClient(sharded_cluster.connection)
    check_index_moved_sharded(client, ["c1"], "c1b", "a_idx", sharded=True)
    check_index_moved_sharded(client, ["c2"], "c2b", "b_idx", sharded=False)
    check_index_moved_sharded(client, ["c3", "c3a"], "c3b", "c_idx", sharded=False)
    check_index_moved_sharded(client, ["c4", "c4a"], "c4b", "d_idx", sharded=True)
