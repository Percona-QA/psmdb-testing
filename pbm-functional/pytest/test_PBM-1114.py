import pytest
import pymongo
import docker

from cluster import Cluster

documents = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

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
        result = cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio:21114 --wait")
        Cluster.log("Setup PBM with nginx proxy:\n" + result.stdout)
        assert result.rc == 0
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(300, func_only=True)
def test_logical_PBM_T266(start_cluster, cluster):
    test = 0
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    result = cluster.exec_pbm_cli("backup --wait")
    assert result.rc != 0, result.stdout + result.stderr
