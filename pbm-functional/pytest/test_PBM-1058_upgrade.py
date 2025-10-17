import pytest
import pymongo
import docker
import platform

from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    if platform.machine() != 'x86_64':
        pytest.skip("Unsupported architecture")
    return Cluster(config,mongod_datadir="/var/lib/mongo/")

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.downgrade(tarball='https://downloads.percona.com/downloads/percona-backup-mongodb/percona-backup-mongodb-2.0.4/binary/tarball/percona-backup-mongodb-2.0.4-x86_64.tar.gz')
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.skip
@pytest.mark.timeout(300,func_only=True)
def test_physical_PBM_T235(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.upgrade()
    cluster.check_pbm_status()
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

