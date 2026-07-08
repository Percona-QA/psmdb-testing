import concurrent.futures
import pytest
import pymongo
import time
import os
import threading
import random

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
        client.admin.command("enableSharding", "test")
        client.test.create_collection('test1',timeseries={'timeField':'timestamp','metaField': 'data'})
        client.test.create_collection('test2',timeseries={'timeField':'timestamp','metaField': 'data'})
        client.admin.command("shardCollection", "test.test1", key={"timestamp": 1})
        client.test["test2"].create_index([("data",pymongo.HASHED )])
        client.admin.command("shardCollection", "test.test2", key={"data": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.skip(reason="Not yet supported")
@pytest.mark.timeout(600,func_only=True)
def test_logical_PBM_T252(start_cluster,cluster):
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_one({"timestamp": datetime.now(), "data": i})
        pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_one({"timestamp": datetime.now(), "data": i})
        time.sleep(0.1)
    backup=cluster.make_backup("logical")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.skip(reason="Not yet supported")
@pytest.mark.timeout(600,func_only=True)
def test_logical_without_data(start_cluster,cluster):
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    backup=cluster.make_backup("logical")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_logical_PBM_T261(start_cluster,cluster):
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    result=cluster.exec_pbm_cli('backup -t logical --wait')
    assert result.rc != 0, result.stdout
    assert 'check for timeseries: cannot backup following sharded timeseries: test.test1, test.test2' in result.stderr
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_incremental_PBM_T262(start_cluster,cluster):
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_one({"timestamp": datetime.now(), "data": i})
        time.sleep(0.1)
    cluster.make_backup("incremental --base")
    for i in range(10):
        pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_one({"timestamp": datetime.now(), "data": i})
        time.sleep(0.1)
    backup=cluster.make_backup("incremental")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == 10
    assert pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({}) == 10
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test2").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.fixture(scope="function")
def start_cluster_unsharded_ts(cluster,request):
    try:
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


# NOTE: PBM does not support the backing up of sharded timeseries collections
@pytest.mark.timeout(600,func_only=True)
def test_logical_pitr_unsharded_timeseries_PBM_366(start_cluster_unsharded_ts,cluster):
    """Verify continuous writes to unsharded timeseries collections will be restored without data loss."""
    client = pymongo.MongoClient(cluster.connection)
    client["test"].create_collection('ts1', timeseries={'timeField': 'timestamp'})

    stop_event = threading.Event()

    def writer(col_name):
        local_client = pymongo.MongoClient(cluster.connection)
        coll = local_client["test"][col_name]
        counter = 0
        while not stop_event.is_set():
            coll.insert_one({"timestamp": datetime.utcnow(), "x": random.randint(0, 1 << 30)})
            counter += 1
        local_client.close()
        return counter

    executor = concurrent.futures.ThreadPoolExecutor()
    futures = {'ts1': executor.submit(writer, 'ts1')}

    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    client["test"].create_collection('ts2', timeseries={'timeField': 'timestamp'})
    futures['ts2'] = executor.submit(writer, 'ts2')

    Cluster.log("Generating timeseries data for 30 seconds")
    time.sleep(30)

    stop_event.set()
    counters = {col: future.result() for col, future in futures.items()}
    executor.shutdown()

    cluster.disable_pitr()

    time.sleep(6)

    pitr_end = cluster.get_last_pitr_chunk_end()
    pitr_time = datetime.utcfromtimestamp(pitr_end).strftime("%Y-%m-%dT%H:%M:%S")

    client["test"].drop_collection('ts1')
    client["test"].drop_collection('ts2')

    cluster.make_restore("--time=" + pitr_time, check_pbm_status=True)

    restored_client = pymongo.MongoClient(cluster.connection)
    assert restored_client["test"]["ts1"].count_documents({}) == counters['ts1']
    assert restored_client["test"]["ts2"].count_documents({}) == counters['ts2']
    Cluster.log("Finished successfully")
