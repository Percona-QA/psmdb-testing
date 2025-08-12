import os
import pytest
import docker

from cluster import Cluster
from perconalink import Perconalink
from data_generator import generate_dummy_data
from data_integrity_check import compare_data_rs

# Sample test for PLM perfomance testing, to run this test, you need to
# 1. Rename sample_rs_plink_PML_T1000 to test_rs_plink_PML_T1000
# 2. For the first time run test as following:
# docker-compose run --env SETUP=true test pytest -s test_load.py
# 3. All subsequent runs should be run as following:
# docker-compose run test pytest -s test_load.py
# 4. To remove running MongoDB instances run as following:
# docker-compose run --env CLEANUP=true test pytest -s test_load.py


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="session")
def plink(cluster_manager):
    srcRS, dstRS = cluster_manager
    return Perconalink("plink", srcRS.plink_connection, dstRS.plink_connection)


@pytest.fixture(scope="session")
def cluster_manager():
    setup = os.environ.get("SETUP", "").lower() == "true"
    cleanup = os.environ.get("CLEANUP", "").lower() == "true"
    srcRS = Cluster({"_id": "rs1", "members": [{"host": "rs101"}]})
    dstRS = Cluster({"_id": "rs2", "members": [{"host": "rs201"}]})
    if setup:
        srcRS.create()
        dstRS.create()
    yield srcRS, dstRS
    if cleanup:
        srcRS.destroy()
        dstRS.destroy()


def initialize_data(srcRS):
    generate_dummy_data(srcRS.connection, "dummy1", 7, 1000000, 10000)
    generate_dummy_data(srcRS.connection, "dummy2", 7, 200000, 10000)
    generate_dummy_data(srcRS.connection, "dummy3", 7, 1500000, 10000)
    generate_dummy_data(srcRS.connection, "dummy4", 7, 100000, 10000)


def sample_rs_plink_PML_T1000(cluster_manager, plink):
    if os.environ.get("SETUP", "").lower() == "true":
        srcRS, _ = cluster_manager
        initialize_data(srcRS)
    if os.environ.get("CLEANUP", "").lower() == "true":
        plink.destroy()
        return
    plink_env = {}
    plink.create(extra_args="--reset-state", env_vars=plink_env)
    srcRS, dstRS = cluster_manager
    assert plink.start() is True, "Failed to start plink"
    assert plink.wait_for_repl_stage(timeout=800) is True, "Failed to finish initial sync"
    assert plink.wait_for_zero_lag(timeout=800) is True, "Failed to catch up on replication"
    assert plink.finalize() is True, "Failed to finalize plink"

    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after sync"
