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
def test_logical_PBM_T305(start_cluster,cluster):
    '''
    The test reproduces the error 'Location6493100: Time monotonicity violation'
    after successful selective logical restore
    '''
    client=pymongo.MongoClient(cluster.connection)
    client.admin.command({"enableSharding": "testDB", "primaryShard": "rs1"})
    client.admin.command({"shardCollection": "testDB.test1", "key": {"_id": 1}})
    client.admin.command({"shardCollection": "testDB.test2", "key": {"_id": 1}})
    client['testDB']['test1'].insert_one({})
    assert client["testDB"]["test1"].count_documents({}) == 1
    assert client["testDB"]["test2"].count_documents({}) == 0

    backup = cluster.make_backup('logical')

    client.drop_database('testDB')
    client.admin.command({"enableSharding": "testDB", "primaryShard": "rs1"})
    client.admin.command({"shardCollection": "testDB.test1", "key": {"_id": 1}})
    client.admin.command({"shardCollection": "testDB.test2", "key": {"_id": 1}})
    client['testDB']['test2'].insert_one({})
    assert client["testDB"]["test1"].count_documents({}) == 0
    assert client["testDB"]["test2"].count_documents({}) == 1

    restore = backup + ' --ns=testDB.test1'
    cluster.make_restore(restore)
    try:
        assert client["testDB"]["test1"].count_documents({}) == 1
        assert client["testDB"]["test2"].count_documents({}) == 1
        assert client["testDB"].command("collstats", "test1").get("sharded", False)
        assert client["testDB"].command("collstats", "test2").get("sharded", False)
    except pymongo.errors.AutoReconnect as e:
        mongos_logs = docker.from_env().containers.get('mongos').logs(tail=100).decode("utf-8", errors="replace")
        assert False, "Mongos failure - pymongo assertion error: " + str(e) + "\n\nMongos logs:\n" + mongos_logs
    Cluster.log("Finished successfully")
