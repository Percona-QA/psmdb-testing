import pytest
import pymongo

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        yield

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def create_index_with_options(connection,option):
    client = pymongo.MongoClient(connection)['test']['test']
    if option == 'collation':
        index = client.create_index("test_field", name="test_index", collation={"locale":"en", "strength": 2})
    elif option == 'sparse':
        index = client.create_index("test_field", name="test_index", sparse=True)
    elif option == 'unique':
        index = client.create_index("test_field", name="test_index", unique=True)
    elif option == 'partialFilterExpression':
        index = client.create_index("test_field", name="test_index", partialFilterExpression={"test_field":{"$exists": True}})
    elif option == 'expireAfterSeconds':
        index = client.create_index("test_field", name="test_index", expireAfterSeconds=3600)
    elif option == 'min':
        index = client.create_index([("test_field", pymongo.GEO2D)], name="test_index", min=-180)
    elif option == 'max':
        index = client.create_index([("test_field", pymongo.GEO2D)], name="test_index", max=180)
    elif option == 'wildcardProjection':
        index = client.create_index("$**", name="test_index", wildcardProjection={"_id": 1, "test_field": 0})
    return index

@pytest.mark.parametrize("index_option", ["collation","sparse","unique","partialFilterExpression","expireAfterSeconds","min","max","wildcardProjection"])
@pytest.mark.timeout(300,func_only=True)
def test_logical_PBM_T288(start_cluster,cluster,index_option):
    cluster.check_pbm_status()
    index = create_index_with_options(cluster.connection,index_option)
    backup = cluster.make_backup("logical")
    cluster.make_restore(backup,make_resync=False,check_pbm_status=False)
    indexes = pymongo.MongoClient(cluster.connection)['test']['test'].index_information()
    assert index in indexes
    assert index_option in indexes['test_index']
    Cluster.log("Finished successfully")

