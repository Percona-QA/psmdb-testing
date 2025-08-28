import pytest
import pymongo

from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

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
def newconfig():
    return { "mongos": "newmongos",
             "configserver":
                            {"_id": "newrscfg", "members": [{"host":"newrscfg01"}]},
             "shards":[
                            {"_id": "newrs1", "members": [{"host":"newrs101"}]},
                            {"_id": "newrs2", "members": [{"host":"newrs201"}]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="package")
def newcluster(newconfig):
    return Cluster(newconfig)

@pytest.fixture(scope="function")
def start_cluster(cluster,newcluster,request):
    try:
        cluster.destroy()
        newcluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        newcluster.create()
        newcluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            newcluster.get_logs()
        cluster.destroy()
        newcluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600,func_only=True)
def test_logical_PBM_T211(start_cluster,cluster,newcluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("logical")
    backup = backup + ' --replset-remapping="newrs1=rs1,newrs2=rs2,newrscfg=rscfg"'
    cluster.destroy()

    newcluster.make_resync()
    newcluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(newcluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newcluster.connection)["test"].command("collstats", "test").get("sharded", False)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_physical_PBM_T242(start_cluster,cluster,newcluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    backup = backup + ' --replset-remapping="newrs1=rs1,newrs2=rs2,newrscfg=rscfg"'
    cluster.destroy()

    newcluster.make_resync()
    newcluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(newcluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newcluster.connection)["test"].command("collstats", "test").get("sharded", False)

@pytest.mark.timeout(600,func_only=True)
def test_incremental_PBM_T243(start_cluster,cluster,newcluster):
    cluster.check_pbm_status()
    cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("incremental")
    backup = backup + ' --replset-remapping="newrs1=rs1,newrs2=rs2,newrscfg=rscfg"'
    cluster.destroy()

    newcluster.make_resync()
    newcluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(newcluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(newcluster.connection)["test"].command("collstats", "test").get("sharded", False)
    print("\nFinished successfully\n")

