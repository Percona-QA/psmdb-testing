import pytest
import pymongo

from datetime import datetime
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical_PBM_T303(start_cluster,cluster):
    """
    STR:
    1. Create 2 time series collections with the same collection name, and make second one empty
    2. Execute logical backup
    """
    client=pymongo.MongoClient(cluster.connection)
    client.test1.create_collection('test',timeseries={'timeField':'timestamp','metaField': 'data'})
    client.test2.create_collection('test',timeseries={'timeField':'timestamp','metaField': 'data'})
    client["test1"]["test"].insert_one({"timestamp": datetime.now(), "data": 1})
    client["test2"]["test"].insert_one({"timestamp": datetime.now(), "data": 2})
    client["test2"]["test"].delete_many({})
    backup=cluster.make_backup("logical")
    client.drop_database('test1')
    client.drop_database('test2')
    cluster.make_restore(backup, check_pbm_status=True)
    assert client["test1"]["test"].count_documents({}) == 1
    assert client["test2"]["test"].count_documents({}) == 0
    Cluster.log("Finished successfully")
