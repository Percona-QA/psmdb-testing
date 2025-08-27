import pytest
import pymongo
import os
import docker

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
        yield

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(600,func_only=True)
def test_logical_PBM_T297(start_cluster,cluster):
    client=pymongo.MongoClient(cluster.connection)
    client.admin.command({"enableSharding": "testDB", "primaryShard": "rs1"})
    client.admin.command({"shardCollection": "testDB.test", "key": {"_id": 1}})
    client['testDB']['test'].insert_one({})

    backup = cluster.make_backup('logical')

    client.drop_database('testDB')
    # it's important to recreate db with the same primary shard
    client.admin.command({"enableSharding": "testDB", "primaryShard": "rs1"})
    client.admin.command({"shardCollection": "testDB.test", "key": {"_id": 1}})

    cluster.make_restore(backup)
    try:
        count = client["testDB"]["test"].count_documents({})
        assert count == 1
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Mongos failure - pymongo assertion error: " + str(e) + "\n\nMongos logs:\n" + mongos_logs
    assert client["testDB"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
