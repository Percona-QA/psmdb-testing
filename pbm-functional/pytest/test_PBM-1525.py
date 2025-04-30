import pytest
import time

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm('/etc/pbm-1525.conf')
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.parametrize('backup_type',['logical','physical'])
def test_s3_custom_ssl_PBM_T296(start_cluster, cluster, backup_type):
    cluster.check_pbm_status()
    backup=cluster.make_backup(backup_type)
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    Cluster.log('Finished succesfully')

