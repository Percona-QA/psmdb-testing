import pytest
import pymongo
import docker
import threading

from cluster import Cluster
from clustersync import Clustersync
from data_integrity_check import compare_data_sharded
from data_generator import create_all_types_db, stop_all_crud_operations

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
def csync(srcCluster,dstCluster):
    return Clustersync('csync',srcCluster.csync_connection, dstCluster.csync_connection)

@pytest.fixture(scope="function")
def start_cluster(srcCluster, dstCluster, csync, request):
    try:
        srcCluster.destroy()
        dstCluster.destroy()
        src_create_thread = threading.Thread(target=srcCluster.create)
        dst_create_thread = threading.Thread(target=dstCluster.create)
        src_create_thread.start()
        dst_create_thread.start()
        src_create_thread.join()
        dst_create_thread.join()
        csync.create()
        yield True

    finally:
        srcCluster.destroy()
        dstCluster.destroy()
        csync.destroy()

def test_sharded_csync_basic(start_cluster, srcCluster, dstCluster, csync):
    """
    Test basic functionality of PCSM with sharded clusters
    """
    srcRS = pymongo.MongoClient(srcCluster.connection)
    operation_threads_1, operation_threads_2, operation_threads_3 = [], [], []
    try:
        _, operation_threads_1 = create_all_types_db(srcCluster.connection, "init_test_db", create_ts=True, start_crud=True)
        assert csync.start(), "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(srcCluster.connection, "clone_test_db", create_ts=True, start_crud=True)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcCluster.connection, "repl_test_db", create_ts=True, start_crud=True)
    finally:
        stop_all_crud_operations()
        for t in operation_threads_1 + operation_threads_2 + operation_threads_3:
            t.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication after resuming from failure"
    assert csync.finalize(), "Failed to finalize csync service"

    # This step is required to ensure that all data is synchronized
    # except for time-series collections which are not supported
    databases = ["init_test_db", "clone_test_db", "repl_test_db"]
    for db in databases:
        srcRS[db].drop_collection("timeseries_data")
    result = compare_data_sharded(srcCluster, dstCluster)
    assert result is True, "Data mismatch after synchronization"
    no_errors, error_logs = csync.check_csync_errors()
    assert no_errors is True, f"csync reported errors in logs: {error_logs}"
