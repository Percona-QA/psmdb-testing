import pytest
import pymongo
import time
import os
import docker

from datetime import datetime
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]},
                            {"_id": "rs2", "members": [{"host":"rs201"}]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600,func_only=True)
@pytest.mark.parametrize('restore_type',['full','selective'])
@pytest.mark.parametrize('primary_shard',['unchanged','changed'])
@pytest.mark.parametrize('old_collection',['sharded','unsharded'])
@pytest.mark.parametrize('new_collection',['sharded','unsharded'])
def test_disabled_drop_pitr_PBM_T281(start_cluster,cluster,restore_type,primary_shard,old_collection,new_collection):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    # the primary shard for old database - rs1
    client.admin.command({ "enableSharding": "testdb", "primaryShard": "rs1" })
    # shard old collection or not
    if old_collection == 'sharded':
        client.admin.command("shardCollection", "testdb.test", key={"_id": "hashed"})

    for i in range(100):
        client["testdb"]["test"].insert_one({"key": i, "data": i})
    full_backup = cluster.make_backup("logical")
    selective_backup = cluster.make_backup("logical --ns=testdb.test")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.05")
    time.sleep(4)
    client.drop_database("testdb")

    # recreate the database with new primary shard or with the same
    match primary_shard:
        case 'unchanged':
            client.admin.command({ "enableSharding": "testdb", "primaryShard": "rs1" })
        case 'changed':
            client.admin.command({ "enableSharding": "testdb", "primaryShard": "rs2" })

    # shard new collection or not
    if new_collection == 'sharded':
        client.admin.command("shardCollection", "testdb.test", key={"_id": "hashed"})

    for i in range(100):
        client["testdb"]["test"].insert_one({"key": i + 100, "data": i + 100})
    time.sleep(4)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    pitr = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(4)
    cluster.disable_pitr()
    assert client["testdb"]["test"].count_documents({}) == 100
    for i in range(100):
        assert client["testdb"]["test"].find_one({"key": i + 100, "data": i + 100})
    client.close()

    # perform full pitr restore or selective
    match restore_type:
        case 'full':
            backup = " --base-snapshot=" + full_backup + pitr
        case 'selective':
            backup = " --base-snapshot=" + selective_backup + pitr + " --ns=testdb.test"
    cluster.make_restore(backup)
    client = pymongo.MongoClient(cluster.connection)
    try:
        assert client["testdb"]["test"].count_documents({}) == 100
    # catch the logs from mongos on connection timeout error
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs().decode("utf-8", errors="replace")
        assert False, str(e) + "\nMongos logs:\n"  +str(mongos_logs)
    for i in range(10):
        assert client["testdb"]["test"].find_one({"key": i + 100, "data": i + 100})
