import subprocess
import time

import pytest

from cluster import Cluster


@pytest.fixture(scope="function")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}

@pytest.fixture(scope="function")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy(cleanup_backups=True)
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(400, func_only=True)
def test_pitr_simultaneous_backups(start_cluster, cluster):
    """
    Verify simultaneous backup commands issued approximately 1 second apart should not leave an agent stuck, blocking PITR from resuming after the backup completes.
    """
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    print("SLEEPING")
    time.sleep(30)

    print("GO!")
    time.sleep(3600)

    host1, host2 = cluster.pbm_hosts[0], cluster.pbm_hosts[1]

    # cluster.wait_pitr(wait=60)

    p1 = subprocess.Popen(["docker", "exec", host1, "pbm", "backup"])
    time.sleep(0.2)
    p2 = subprocess.Popen(["docker", "exec", host2, "pbm", "backup"])
    p1.wait()
    p2.wait()

    cluster.wait_pitr(wait=60)
