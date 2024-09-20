import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker

from datetime import datetime
from cluster import Cluster
from packaging import version


@pytest.fixture(scope="package")
def mongod_version():
    return docker.from_env().containers.run(
                    image='replica_member/local',
                    remove=True,
                    command='bash -c \'mongod --version | head -n1 | sed "s/db version v//"\''
          ).decode("utf-8", errors="replace")

@pytest.fixture(scope="package")
def config(mongod_version):
    if version.parse(mongod_version) < version.parse("8.0.0"):
        pytest.skip("Unsupported version for unshardCollection")
    else:
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
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        cluster.setup_pbm()
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups --set backup.compression=none")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(900,func_only=True)
def test_logical_PBM_T264(start_cluster,cluster):
    cluster.check_pbm_status()
    client=pymongo.MongoClient(cluster.connection)
    for i in range(600):
        client['test']['test'].insert_one({"doc":i})

    cluster.make_backup('logical')
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    time.sleep(10)
    Cluster.log("Start unsharding collection test.test")
    result=client.admin.command({'unshardCollection': "test.test", 'toShard': "rs2"})
    Cluster.log(result)
    time.sleep(10)
    assert not pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Time for PITR is: " + pitr)
    pitr_backup="--time=" + pitr
    time.sleep(60)
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(pitr_backup,check_pbm_status=True,make_resync=False)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 600
    assert not pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)

