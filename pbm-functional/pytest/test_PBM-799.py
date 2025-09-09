import pytest
import pymongo
import os

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"},{"host": "rs102"},{"host": "rs103"},
                                      {"host": "rs104", "hidden": True, "priority": 0, "votes": 0},
                                      {"host": "rs105", "hidden": True, "priority": 0, "votes": 0},
                                      {"host": "rs106", "hidden": True, "priority": 0, "votes": 0},
                                      {"host": "rs107", "hidden": True, "priority": 0, "votes": 0}
                                     ]}

@pytest.fixture(scope="package")
def pbm_mongodb_uri():
    return 'mongodb://pbm:pbmpass@127.0.0.1:27017/?authSource=admin&w=3&readConcernLevel=local'

@pytest.fixture(scope="package")
def cluster(config,pbm_mongodb_uri):
    return Cluster(config, pbm_mongodb_uri=pbm_mongodb_uri)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.setup_pbm("/etc/pbm-fs.conf")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical_PBM_T254(start_cluster,cluster):
    for i in range(100):
        pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({"a": i})
    backup = cluster.make_backup("logical")
    backup_status = cluster.exec_pbm_cli("describe-backup " + backup).stdout
    Cluster.log("Backup status: \n" + backup_status)
    cluster.make_resync()

    cluster.check_pbm_status()
    backup_status = cluster.exec_pbm_cli("describe-backup " + backup).stdout
    Cluster.log("Backup status: \n" + backup_status)
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 100
    Cluster.log("Finished successfully")

