import pytest
import pymongo
import time
import os
import docker

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
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test3", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600,func_only=True)
def test_logical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_many(documents)
    backup_full=cluster.make_backup("logical")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup_full,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test1").get("sharded", True) is False
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")


@pytest.mark.timeout(300, func_only=True)
def test_logical_selective_PBM_T218(start_cluster, cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    client.admin.command("enableSharding", "test2")
    client.admin.command("shardCollection", "test2.test_coll21", key={"_id": "hashed"})
    for i in range(10):
        client["test1"]["test_coll11"].insert_one({"key": i, "data": i})
        client["test2"]["test_coll21"].insert_one({"key": i, "data": i})
        client["test2"]["test_coll22"].insert_one({"key": i, "data": i})
    client["test1"]["test_coll11"].create_index(["key"], name="test_coll11_index_old")
    client["test2"]["test_coll21"].create_index(["key"], name="test_coll21_index_old")
    backup_full = cluster.make_backup("logical")
    backup_partial = cluster.make_backup("logical --ns=test1.test_coll11,test2.*")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    client["test1"]["test_coll11"].drop_index('test_coll11_index_old')
    client["test1"]["test_coll11"].delete_many({})
    for i in range(10):
        client["test1"]["test_coll11"].insert_one({"key": i + 10, "data": i + 10})
    client["test1"]["test_coll11"].create_index("data", name="test_coll11_index_new")
    client["test2"]["test_coll22"].create_index("data", name="test_coll22_index_new")
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    pitr = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr()
    time.sleep(5)
    client.drop_database("test1")
    client.drop_database("test2")
    backup_partial = " --base-snapshot=" + backup_partial + pitr
    backup_full = (
        " --base-snapshot=" + backup_full + pitr + " --ns=test1.test_coll11,test2.*"
    )
    cluster.make_restore(backup_partial, check_pbm_status=True)
    assert client["test1"]["test_coll11"].count_documents({}) == 10
    assert client["test1"].command("collstats", "test_coll11").get("sharded", True) is False
    assert client["test2"]["test_coll21"].count_documents({}) == 10
    assert client["test2"].command("collstats", "test_coll21").get("sharded", False)
    assert client["test2"]["test_coll22"].count_documents({}) == 10
    assert client["test2"].command("collstats", "test_coll22").get("sharded", True) is False
    for i in range(10):
        assert client["test1"]["test_coll11"].find_one({"key": i + 10, "data": i + 10})
        assert client["test2"]["test_coll21"].find_one({"key": i, "data": i})
        assert client["test2"]["test_coll22"].find_one({"key": i, "data": i})
    assert "test_coll11_index_old" not in client["test1"]["test_coll11"].index_information()
    assert "test_coll11_index_new" in client["test1"]["test_coll11"].index_information()
    assert "test_coll21_index_old" in client["test2"]["test_coll21"].index_information()
    assert "test_coll22_index_new" in client["test2"]["test_coll22"].index_information()
    client.drop_database("test1")
    client.drop_database("test2")
    cluster.make_restore(backup_full, check_pbm_status=True)
    assert client["test1"]["test_coll11"].count_documents({}) == 10
    assert client["test1"].command("collstats", "test_coll11").get("sharded", True) is False
    assert client["test2"]["test_coll21"].count_documents({}) == 10
    assert client["test2"].command("collstats", "test_coll21").get("sharded", False)
    assert client["test2"]["test_coll22"].count_documents({}) == 10
    assert client["test2"].command("collstats", "test_coll22").get("sharded", True) is False
    for i in range(10):
        assert client["test1"]["test_coll11"].find_one({"key": i + 10, "data": i + 10})
        assert client["test2"]["test_coll21"].find_one({"key": i, "data": i})
        assert client["test2"]["test_coll22"].find_one({"key": i, "data": i})
    assert "test_coll11_index_old" not in client["test1"]["test_coll11"].index_information()
    assert "test_coll11_index_new" in client["test1"]["test_coll11"].index_information()
    assert "test_coll21_index_old" in client["test2"]["test_coll21"].index_information()
    assert "test_coll22_index_new" in client["test2"]["test_coll22"].index_information()
    Cluster.log("Finished successfully")


@pytest.mark.timeout(600, func_only=True)
def test_logical_pitr_PBM_T194(start_cluster,cluster):
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    # make several following backups and then remove them to check the continuity of PITR timeframe
    pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_many(documents)
    backup_l2 = cluster.make_backup("logical")
    time.sleep(5)
    pymongo.MongoClient(cluster.connection)["test"]["test3"].insert_many(documents)
    backup_l3 = cluster.make_backup("logical")
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(5)
    cluster.delete_backup(backup_l2)
    cluster.delete_backup(backup_l3)
    cluster.disable_pitr()
    time.sleep(5)
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test3"].count_documents({}) == len(documents)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_physical(start_cluster,cluster):
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
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("physical")
    time.sleep(5)
    pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_many(documents)
    pymongo.MongoClient(cluster.connection)["test"]["test3"].insert_many(documents)
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr + " --base-snapshot=" + backup
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(5)
    cluster.disable_pitr()
    time.sleep(5)
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
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.external_backup_start()
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.external_backup_copy(backup)
    cluster.external_backup_finish(backup)
    time.sleep(5)
    os.system("find /backups/ -name pbm.rsmeta.* | xargs rm -f")
    restore=cluster.external_restore_start()
    cluster.external_restore_copy(backup)
    cluster.external_restore_finish(restore)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
