import pytest
import pymongo
import bson
import threading
import os

from cluster import Cluster


@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}


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
        result = cluster.exec_pbm_cli(
            "config --set storage.type=filesystem --set storage.filesystem.path=/backups "
            "--set backup.compression=none --out json --wait"
        )
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        yield True

    finally:
        print()
        Cluster.log("PBM logs:\n" + cluster.exec_pbm_cli("logs -t 0 -sD").stdout)
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(300, func_only=True)
def test_load_PBM_T204(start_cluster, cluster):
    cluster.check_pbm_status()

    client = pymongo.MongoClient(cluster.connection)
    db = client.test
    collection = db.test
    Cluster.log("Create collection, unique index and insert data")
    collection.insert_one({"a": 1, "b": 1, "c": 1})
    collection.create_index([("a", 1), ("b", 1), ("c", 1)], name="test_index", unique=True)
    res = pymongo.MongoClient(cluster.connection)["test"]["test"].find({})
    Cluster.log("Collection:")
    for r in res:
        Cluster.log(r)

    def upsert_1():
        Cluster.log("Starting background upsert 1")
        while upsert:
            query = {"a": 1}
            update = {"$set": {"a": 1, "b": 1, "c": 1}}
            pymongo.MongoClient(cluster.connection)["test"]["test"].delete_one(query)
            try:
                doc = pymongo.MongoClient(cluster.connection)["test"]["test"].find_one_and_update(
                    query, update, upsert=True, return_document=pymongo.collection.ReturnDocument.AFTER
                )
                # Cluster.log(doc)
            except pymongo.errors.DuplicateKeyError:
                pass
        Cluster.log("Stopping background upsert 1")

    def upsert_2():
        Cluster.log("Starting background upsert 2")
        while upsert:
            query = {"b": 1}
            update = {"$set": {"a": 2, "b": 1, "c": 1}}
            pymongo.MongoClient(cluster.connection)["test"]["test"].delete_one(query)
            try:
                doc = pymongo.MongoClient(cluster.connection)["test"]["test"].find_one_and_update(
                    query, update, upsert=True, return_document=pymongo.collection.ReturnDocument.AFTER
                )
                # Cluster.log(doc)
            except pymongo.errors.DuplicateKeyError:
                pass
        Cluster.log("Stopping background upsert 2")

    upsert = True
    t1 = threading.Thread(target=upsert_1)
    t2 = threading.Thread(target=upsert_2)
    t1.start()
    t2.start()

    backup = cluster.make_backup("logical")

    upsert = False
    t2.join()
    t1.join()

    res = pymongo.MongoClient(cluster.connection)["test"]["test"].find({})
    Cluster.log("Collection:")
    for r in res:
        Cluster.log(r)

    try:
        cluster.make_restore(backup, check_pbm_status=True)
    except AssertionError:
        file = "/backups/" + backup + "/rs1/local.oplog.rs.bson"
        with open(file, "rb") as f:
            data = f.read()
            docs = bson.decode_all(data)
            for doc in docs:
                print(doc)
        assert False

    res = pymongo.MongoClient(cluster.connection)["test"]["test"].find({})
    Cluster.log("Collection:")
    for r in res:
        Cluster.log(r)

    Cluster.log("Finished successfully\n")
