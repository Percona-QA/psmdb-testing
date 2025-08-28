import pytest
import pymongo
import os
import random
import string

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config, mongod_extra_args="--directoryperdb")

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        pymongo.MongoClient(cluster.connection).admin.command( { "setParameter": 1, "wiredTigerEngineRuntimeConfig": "cache_size=4G"} )
        cluster.setup_pbm("/etc/pbm-fs.conf")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(3600,func_only=True)
def test_load_PBM_T250(start_cluster,cluster):
    cluster.check_pbm_status()
    indexes = []

    for i in range(63):
        indexes.append(pymongo.IndexModel(str(i),background=True))

    for i in range(1500):
        database = ''.join(random.choice(string.ascii_lowercase) for _ in range(63))
        client=pymongo.MongoClient(cluster.connection)
        db = client[database]
        db.create_collection("test_collection")
        db["test_collection"].create_indexes(indexes)
        Cluster.log( database + ": " + str(i))

    cluster.make_backup("physical")
#    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True,timeout=1200)
    Cluster.log("Finished successfully")

