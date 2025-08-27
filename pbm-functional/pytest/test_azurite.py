import pytest
import pymongo
import time

from datetime import datetime
from cluster import Cluster

documents = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def config():
    return {"mongos": "mongos",
            "configserver":
            {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
            "shards": [
                {"_id": "rs1", "members": [{"host": "rs101"}]},
                {"_id": "rs2", "members": [{"host": "rs201"}]}
            ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        azure_account = "devstoreaccount1"
        azure_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        azure_endpoint = "http://azurite:10000/" + azure_account
        result = cluster.exec_pbm_cli("config --set storage.type=azure --set storage.azure.endpointUrl=" + azure_endpoint +
                                      " --set storage.azure.account=" + azure_account +
                                      " --set storage.azure.container=test-container --set storage.azure.credentials.key=" + azure_key +
                                      " --out json --wait")
        Cluster.log("Setup PBM with azurite storage:\n" + result.stdout)
        assert result.rc == 0
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command(
            "shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command(
            "shardCollection", "test.test2", key={"_id": "hashed"})
        client.admin.command(
            "shardCollection", "test.test3", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300, func_only=True)
def test_logical(start_cluster, cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("logical")
    result = pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")

@pytest.mark.timeout(500, func_only=True)
def test_logical_pitr(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    # make several following backups and then remove them to check the continuity of PITR timeframe
    pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_many(documents)
    backup_l2=cluster.make_backup("logical")
    time.sleep(5)
    pymongo.MongoClient(cluster.connection)["test"]["test3"].insert_many(documents)
    backup_l3=cluster.make_backup("logical")
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

@pytest.mark.timeout(300, func_only=True)
def test_physical(start_cluster, cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("physical")
    result = pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup, restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")

@pytest.mark.timeout(300, func_only=True)
def test_incremental(start_cluster, cluster):
    cluster.check_pbm_status()
    cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.make_backup("incremental")
    result = pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup, restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully\n")
