import pytest
import pymongo
import time
import os

from datetime import datetime
from cluster import Cluster

documents = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {
            "_id": "rscfg",
            "members": [{"host": "rscfg01"}, {"host": "rscfg02"}, {"host": "rscfg03"}],
        },
        "shards": [
            {
                "_id": "rs1",
                "members": [{"host": "rs101"}],
            }
        ],
    }


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(600, func_only=True)
def test_physical_PBM_T278(start_cluster, cluster):
    client = pymongo.MongoClient(cluster.connection)
    client.admin.command("enableSharding", "test")
    client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
    cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    client["test"]["test"].insert_many(documents)
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr(pitr)
    time.sleep(5)
    client.drop_database("test")
    backup = " --time=" + pitr
    cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)
    cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(10)
    assert client["test"]["test"].count_documents({}) == len(documents)
    assert client["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")
