import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading

from datetime import datetime
from cluster import Cluster

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def src_config():
    return { "mongos": "mongos1",
             "configserver":
                            {"_id": "rscfg1", "members": [{"host":"rscfg101"},{"host": "rscfg102"},{"host": "rscfg103" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]}
                      ]}

@pytest.fixture(scope="package")
def dst_config():
    return { "mongos": "mongos2",
             "configserver":
                            {"_id": "rscfg2", "members": [{"host":"rscfg201"},{"host": "rscfg202"},{"host": "rscfg203" }]},
             "shards":[
                            {"_id": "rs3", "members": [{"host":"rs301"},{"host": "rs302"},{"host": "rs303" }]},
                            {"_id": "rs4", "members": [{"host":"rs401"},{"host": "rs402"},{"host": "rs403" }]}
                      ]}

@pytest.fixture(scope="package")
def srcCluster(src_config):
    return Cluster(src_config)

@pytest.fixture(scope="package")
def dstCluster(dst_config):
    return Cluster(dst_config)

@pytest.fixture(scope="function")
def start_cluster(srcCluster, dstCluster,request):
    try:
        srcCluster.destroy()
        dstCluster.destroy()
        srcCluster.create()
        dstCluster.create()
        yield True

    finally:
        srcCluster.destroy()
        dstCluster.destroy()
        Cluster.destroy_mlink()


def test_sharded_mlink_basic(start_cluster, srcCluster, dstCluster):
    src = pymongo.MongoClient(srcCluster.connection)
    dst = pymongo.MongoClient(dstCluster.connection)

    src["test_db1"]["test_coll11"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db1"]["test_coll12"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db2"]["test_coll21"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db2"]["test_coll22"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db1"]["test_coll11"].create_index(["key"], name="test_coll11_index_old")

    mlink_container = Cluster.create_mlink(srcCluster.pml_connection, dstCluster.pml_connection)
    assert mlink_container is not None, "Failed to create mlink service"

    result = Cluster.start_mlink_service(mlink_container)
    assert result is True, "Failed to start mlink service"
    result = Cluster.finalize_mlink_service(mlink_container)
    assert result is True, "Failed to finalize mlink service"

    result = Cluster.compare_data_sharded(srcCluster, dstCluster)
    assert result is True, "Data mismatch after synchronization"

    src["test_db2"]["test_coll21"].insert_many([{"key": i, "data": i} for i in range(10)])
    src["test_db3"]["test_coll31"].insert_many([{"key": i, "data": i} for i in range(10)])
    dst["test_db4"]["test_coll41"].insert_many([{"key": i, "data": i} for i in range(10)])

    src["test_db3"]["test_coll31"].create_index(["key"], name="test_coll31_index_old")
    dst["test_db1"]["test_coll11"].drop_index('test_coll11_index_old')
    dst["test_db1"]["test_coll11"].create_index(["data"], name="test_coll11_index_old")
    dst["test_db1"]["test_coll11"].create_index(["key"], name="test_coll11_index_new")

    result = Cluster.compare_data_sharded(srcCluster, dstCluster)
    assert result is False, "Data should not match after modification in dst"
