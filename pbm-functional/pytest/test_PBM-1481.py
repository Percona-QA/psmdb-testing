import pytest
import time
import os
import docker
import re

from datetime import datetime
from cluster import Cluster


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(300, func_only=True)
def test_logical_backup_and_PITR_timestamp_PBM_T290(start_cluster, cluster):
    cluster.check_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup = "--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(30)
    cluster.make_restore(backup, check_pbm_status=True)

    list_backups = cluster.exec_pbm_cli("list ").stdout
    pbm_status = cluster.exec_pbm_cli("status").stdout

    assert re.search(r"restore_to_time:\s\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?!Z)", list_backups)
    assert re.search(r"restore_to_time:\s\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?!Z)", pbm_status)

    Cluster.log("Finished successfully\n")
