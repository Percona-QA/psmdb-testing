import pytest
import pymongo
import os
import docker
import random

from cluster import Cluster

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host":"rs102"},{"host":"rs103"}]},
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
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(1800,func_only=True)
def test_incremental_PBM_T293(start_cluster,cluster):
    cluster.exec_pbm_cli("config --set restore.downloadChunkMb=1")
    # generate 1024 collections with different size
    for i in range(1024):
        db = "test" + str(i)
        pymongo.MongoClient(cluster.connection)[db]['test'].insert_one({'data': random.randbytes(i*1024)})
        Cluster.log("Filled collection " + db + ".test")
    base_backup = cluster.make_backup("incremental --base")

    try:
        cluster.make_restore(base_backup,timeout=900, restart_cluster=True, check_pbm_status=True)
    except AssertionError as e:
        rs101_logs = docker.from_env().containers.get('rs101').logs(tail=10,stdout=False).decode("utf-8", errors="replace")
        assert False, str(e) + "\n Logs from rs101:\n" + rs101_logs
