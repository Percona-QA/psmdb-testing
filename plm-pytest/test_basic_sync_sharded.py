import pytest
import pymongo
import docker

from cluster import Cluster
from perconalink import Perconalink
from data_integrity_check import compare_data_sharded
from data_generator import create_all_types_db, generate_dummy_data, stop_all_crud_operations

@pytest.fixture(scope="module")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="module")
def src_config():
    return { "mongos": "mongos1",
             "configserver":
                            {"_id": "rscfg1", "members": [{"host":"rscfg101"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]},
                            {"_id": "rs2", "members": [{"host":"rs201"}]}
                      ]}

@pytest.fixture(scope="module")
def dst_config():
    return { "mongos": "mongos2",
             "configserver":
                            {"_id": "rscfg2", "members": [{"host":"rscfg201"}]},
             "shards":[
                            {"_id": "rs3", "members": [{"host":"rs301"}]},
                            {"_id": "rs4", "members": [{"host":"rs401"}]}
                      ]}

@pytest.fixture(scope="module")
def srcCluster(src_config):
    return Cluster(src_config)

@pytest.fixture(scope="module")
def dstCluster(dst_config):
    return Cluster(dst_config)

@pytest.fixture(scope="module")
def plink(srcCluster,dstCluster):
    return Perconalink('plink',srcCluster.plink_connection, dstCluster.plink_connection)

@pytest.fixture(scope="function")
def start_cluster(srcCluster, dstCluster, plink, request):
    try:
        srcCluster.destroy()
        dstCluster.destroy()
        srcCluster.create()
        dstCluster.create()
        plink.create()
        yield True

    finally:
        srcCluster.destroy()
        dstCluster.destroy()
        plink.destroy()

def test_sharded_plink_basic(start_cluster, srcCluster, dstCluster, plink):
    """
    Test basic functionality of PLM with sharded clusters
    """
    srcRS = pymongo.MongoClient(srcCluster.connection)
    operation_threads_1, operation_threads_2, operation_threads_3 = [], [], []
    try:
        generate_dummy_data(srcCluster.connection)
        _, operation_threads_1 = create_all_types_db(srcCluster.connection, "init_test_db", create_ts=True, start_crud=True)
        assert plink.start(), "Failed to start plink service"
        _, operation_threads_2 = create_all_types_db(srcCluster.connection, "clone_test_db", create_ts=True, start_crud=True)
        assert plink.wait_for_repl_stage(), "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcCluster.connection, "repl_test_db", create_ts=True, start_crud=True)
    finally:
        stop_all_crud_operations()
        for t in operation_threads_1 + operation_threads_2 + operation_threads_3:
            t.join()
    assert plink.wait_for_zero_lag(), "Failed to catch up on replication after resuming from failure"
    assert plink.finalize(), "Failed to finalize plink service"

    # This step is required to ensure that all data is synchronized
    # except for time-series collections which are not supported
    databases = ["init_test_db", "clone_test_db", "repl_test_db"]
    for db in databases:
        srcRS[db].drop_collection("timeseries_data")
    result = compare_data_sharded(srcCluster, dstCluster)
    assert result is True, "Data mismatch after synchronization"
    no_errors, error_logs = plink.check_plink_errors()
    assert no_errors is True, f"plink reported errors in logs: {error_logs}"
