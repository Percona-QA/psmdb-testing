import pytest
import pymongo

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config, mongod_extra_args="--directoryperdb")

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

@pytest.mark.timeout(3600,func_only=True)
def test_logical_PBM_T330(start_cluster,cluster):
    """
    Test the correctness of backup size calculation with no compression
    1. Set backup.compression to none
    2. Set maxUploadParts to lower value to reproduce the issue with smaller data sizes
    3. Add highly compressible data to DB
    4. Perform backup/restore
    """
    cluster.exec_pbm_cli('config --set backup.compression=none')
    cluster.exec_pbm_cli('config --set storage.s3.maxUploadParts=5')
    cluster.check_pbm_status()
    data = '0' * (8 * 1024 * 1024)
    client=pymongo.MongoClient(cluster.connection)
    for i in range (1024):
        client["test"]["test"].insert_one({"data": data})
    backup = cluster.make_backup("logical")
    cluster.make_restore(backup)
    Cluster.log("Finished successfully")

