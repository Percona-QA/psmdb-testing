import pytest
import pymongo
import os
import docker

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
        pytest.skip("Unsupported version for config shards")
    else:
        return { "mongos": "mongos",
                 "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"}]},
                 "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.setup_pbm("/etc/pbm-fs.conf")
        client=pymongo.MongoClient(cluster.connection)
        Cluster.log(client.admin.command({'transitionFromDedicatedConfigServer': 1}))
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_logical_selective_PBM_T267(start_cluster,cluster):
    client=pymongo.MongoClient(cluster.connection)
    for i in range(10):
        client['test']['test'].insert_one({"doc":i})
        client['test']['test1'].insert_one({"doc":i})

    result = cluster.exec_pbm_cli("logical --ns=test.test --wait")
    assert result.rc != 0, result.stdout + result.stderr
    result = cluster.exec_pbm_cli("logical --ns=test.test1 --wait")
    assert result.rc != 0, result.stdout + result.stderr

