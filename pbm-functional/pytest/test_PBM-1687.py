import re
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

@pytest.mark.skip(reason="Disable brittle test")
@pytest.mark.timeout(600, func_only=True)
def test_pitr_simultaneous_backups_PBM348(start_cluster, cluster):
    """
    Verify simultaneous backup commands does not leave PITR blocked from resuming after the backup completes
    """
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")

    host1, host2 = cluster.pbm_hosts[0], cluster.pbm_hosts[1]

    race_triggered = False
    for delay in ["0.8", "0.5", "0.3"]:
        cluster.wait_pitr(wait=60)
        cmd = f"docker exec {host1} pbm backup & sleep {delay} && docker exec {host2} pbm backup & wait"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
        output = result.stdout + result.stderr
        Cluster.log(f"delay={delay} output: {output.strip()}")
        backup_names = re.findall(r'Starting backup "([^"]+)"', output)
        if "another operation in progress" not in output and len(set(backup_names)) > 1:
            Cluster.log(f"Race condition triggered at delay={delay}")
            race_triggered = True
            break
        Cluster.log(f"Race not triggered at delay={delay}, retrying with shorter delay")
        timeout = time.time() + 60
        while cluster.get_status()["running"]:
            assert time.time() < timeout, "Backup did not complete within timeout"
            time.sleep(2)

    assert race_triggered, "Could not trigger race condition across all delays"
    cluster.wait_pitr(wait=60)
