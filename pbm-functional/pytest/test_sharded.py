import json
import pytest
import pymongo
import time
import os
import docker
import threading
import testinfra

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

@pytest.fixture(scope="function", params=["/etc/pbm-fs.conf"])
def start_cluster(cluster,request):
    try:
        pbm_config = request.param
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_azurite()
        cluster.setup_pbm(pbm_config)
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test3", key={"_id": "hashed"})
        yield pbm_config

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
    client["test1"]["test_coll11"].drop_index('test_coll11_index_old')
    client["test1"]["test_coll11"].delete_many({})
    for i in range(10):
        client["test1"]["test_coll11"].insert_one({"key": i + 10, "data": i + 10})
    client["test1"]["test_coll11"].create_index("data", name="test_coll11_index_new")
    client["test2"]["test_coll22"].create_index("data", name="test_coll22_index_new")
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr(pitr)
    pitr = " --time=" + pitr
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
    cluster.disable_pitr(pitr)
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
    cluster.disable_pitr(pitr)
    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test3"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_incremental(start_cluster,cluster):

    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    stop_event = threading.Event()

    def continuous_write():
        i = 0
        while not stop_event.is_set():
            client["test"]["test"].insert_one({"doc": i})
            i += 1
            time.sleep(0.05)

    writer = threading.Thread(target=continuous_write)
    writer.start()

    cluster.make_backup("incremental --base")

    for n in range(2):
        cluster.make_backup("incremental")
        time.sleep(1)

    stop_event.set()
    writer.join()

    last_backup = cluster.make_backup("incremental")

    expected_count = client["test"]["test"].count_documents({})
    Cluster.log(f"Documents before restore: {expected_count}")

    client["test"]["test"].drop()
    assert client["test"]["test"].count_documents({}) == 0

    cluster.make_restore(last_backup, restart_cluster=True, check_pbm_status=True)

    actual_count = pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({})
    assert actual_count == expected_count, f"Expected {expected_count} docs after restore, got {actual_count}"
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.fixture(scope="function")
def start_cluster_fs(cluster, request):
    try:
        cluster.destroy()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm("/etc/pbm-fs.conf")
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300, func_only=True)
def test_backup_fails_on_lost_shard(start_cluster_fs, cluster):
    """Verify backup is marked as error when a shard agent becomes unreachable mid-backup"""
    client = pymongo.MongoClient(cluster.connection)

    for start in range(0, 20000, 1000):
        client["test"]["test"].insert_many([{"x": i, "pad": "x" * 500} for i in range(start, start + 1000)])

    result = cluster.exec_pbm_cli("backup --type=logical --out=json")
    backup_name = json.loads(result.stdout)["name"]

    shard_hosts = ["rs101", "rs102", "rs103"]
    shard_nodes = [testinfra.get_host(f"docker://{h}") for h in shard_hosts]
    matching = []
    try:
        timeout = time.time() + 120
        while True:
            status = cluster.get_status()
            if status.get("running", {}).get("type") == "backup":
                break
            assert time.time() < timeout, "Timed out waiting for backup to start"
            time.sleep(1)

        time.sleep(3)
        for node in shard_nodes:
            node.check_output("kill -STOP $(pgrep pbm-agent)")

        timeout = time.time() + 90
        while True:
            status = cluster.get_status()
            snapshots = status.get("backups", {}).get("snapshot", [])
            matching = [s for s in snapshots if s["name"] == backup_name]
            if matching and matching[0]["status"] == "error":
                break
            assert time.time() < timeout, "Timed out waiting for backup to be marked as error"
            time.sleep(2)
    finally:
        for node in shard_nodes:
            try:
                node.check_output("kill -CONT $(pgrep pbm-agent)")
            except Exception:
                pass

    error_msg = matching[0].get("error", "")
    assert "lost shard" in error_msg or "some of pbm-agents were lost during the backup" in error_msg, (f"Expected lost shard/agent error, got: {error_msg}")

