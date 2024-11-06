import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading

from datetime import datetime
from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]}
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
        client.admin.command("enableSharding", "test2")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test3", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test2.test21", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600,func_only=True)
def test_logical_PBM_T218(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_many(documents)
    pymongo.MongoClient(cluster.connection)["test2"]["test21"].insert_many(documents)
    pymongo.MongoClient(cluster.connection)["test2"]["test22"].insert_many(documents)
    backup_partial=cluster.make_backup("logical --ns=test.test,test2.*")
    backup_full=cluster.make_backup("logical")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup_partial,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == 0
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", True) is False
    assert pymongo.MongoClient(cluster.connection)["test2"]["test21"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test2"].command("collstats", "test21").get("sharded", False)
    assert pymongo.MongoClient(cluster.connection)["test2"]["test22"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test2"].command("collstats", "test22").get("sharded", True) is False
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup_full,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", True) is False
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600, func_only=True)
def test_logical_pitr_PBM_T194(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup_l1=cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    time.sleep(60)
    # make several following backups and then remove them to check the continuity of PITR timeframe
    pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_many(documents)
    backup_l2=cluster.make_backup("logical")
    time.sleep(60)
    pymongo.MongoClient(cluster.connection)["test"]["test3"].insert_many(documents)
    backup_l3=cluster.make_backup("logical")
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(60)
    cluster.delete_backup(backup_l2)
    cluster.delete_backup(backup_l3)
    cluster.disable_pitr()
    time.sleep(10)
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test3"].count_documents({}) == len(documents)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_physical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600, func_only=True)
def test_physical_pitr_PBM_T244(start_cluster,cluster):
    cluster.check_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    time.sleep(30)
    pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_many(documents)
    pymongo.MongoClient(cluster.connection)["test"]["test3"].insert_many(documents)
    time.sleep(30)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr + " --base-snapshot=" + backup
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(60)
    cluster.disable_pitr()
    time.sleep(10)
    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test3"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_incremental(start_cluster,cluster):
    cluster.check_pbm_status()
    cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("incremental")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_external_meta_PBM_T236(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.external_backup_start()
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.external_backup_copy(backup)
    cluster.external_backup_finish(backup)
    time.sleep(10)
    restore=cluster.external_restore_start()
    cluster.external_restore_copy(backup)
    cluster.external_restore_finish(restore)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_external_nometa_PBM_T237(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.external_backup_start()
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.external_backup_copy(backup)
    cluster.external_backup_finish(backup)
    time.sleep(10)
    os.system("find /backups/ -name pbm.rsmeta.* | xargs rm -f")
    restore=cluster.external_restore_start()
    cluster.external_restore_copy(backup)
    cluster.external_restore_finish(restore)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
