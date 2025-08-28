import pytest
import pymongo
import os

from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]}
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
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600,func_only=True)
def test_physical_mixed_env_PBM_T248(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)

    #primary cfgsrv - CE, else - PSMDB, backup should pass
    Cluster.psmdb_to_ce("rscfg01")
    cluster.check_pbm_status()
    assert cluster.exec_pbm_cli("backup -t physical --wait").rc == 0
    Cluster.ce_to_psmdb("rscfg01")

    #primary cfgsrv - PSMDB, else - CE, backup should pass
    Cluster.psmdb_to_ce("rscfg02")
    Cluster.psmdb_to_ce("rscfg03")
    cluster.check_pbm_status()
    assert cluster.exec_pbm_cli("backup -t physical --wait").rc == 0
    Cluster.ce_to_psmdb("rscfg02")
    Cluster.ce_to_psmdb("rscfg03")

    #one shard - CE, backup should fail on preflight check
    Cluster.psmdb_to_ce("rs101")
    Cluster.psmdb_to_ce("rs102")
    Cluster.psmdb_to_ce("rs103")
    cluster.check_pbm_status()
    #removed preflight checks on the cli side due to PBM-1512
    assert cluster.exec_pbm_cli("backup -t physical --wait ").rc == 1
#    assert cluster.exec_pbm_cli("backup -t physical").rc == 1
#    assert cluster.exec_pbm_cli("backup -t physical -o json").rc == 1
