import pytest
import pymongo
import time
import os

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
        profile=cluster.exec_pbm_cli("profile add worm /etc/minio-worm.conf --wait")
        assert profile.rc==0, profile.stderr
        assert "OK" in profile.stdout, profile.stdout
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.parametrize('backup_type',['logical','physical'])
def test_worm_profile(start_cluster,cluster,backup_type):
    client=pymongo.MongoClient(cluster.connection)
    for i in range(300):
        client['test']['test'].insert_one({"doc":i})

    backup=cluster.make_backup(backup_type + " --profile worm")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    Cluster.log("Attempt restore " + backup_type + " backup from the worm storage")
    if backup_type == 'logical':
        cluster.make_restore(backup, check_pbm_status=True)
    else:
        cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)
    time.sleep(5)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 300
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
