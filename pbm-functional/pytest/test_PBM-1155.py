import pytest
import pymongo

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config,mongod_extra_args='--setParameter enableTestCommands=1')

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

def configure_failpoint_BackupCursorOpenConflictWithCheckpoint(connection):
    client = pymongo.MongoClient(connection)
    mode = { 'skip': 0, 'times': 3 }
    data = { 'errorCode': 50915, 'failCommands': ['aggregate']}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data})
    Cluster.log(result)

@pytest.mark.timeout(6000, func_only=True)
def test_incremental_base_PBM_T246(start_cluster,cluster):
    configure_failpoint_BackupCursorOpenConflictWithCheckpoint(cluster.connection)
    cluster.make_backup("incremental --base")
    assert 'a checkpoint took place, retrying' in cluster.exec_pbm_cli("logs -sD -t0 -e backup").stdout
    Cluster.log("Finished successfully")

@pytest.mark.timeout(6000, func_only=True)
def test_incremental_PBM_T245(start_cluster,cluster):
    cluster.make_backup("incremental --base")
    configure_failpoint_BackupCursorOpenConflictWithCheckpoint(cluster.connection)
    cluster.make_backup("incremental")
    assert 'a checkpoint took place, retrying' in cluster.exec_pbm_cli("logs -sD -t0 -e backup").stdout
    Cluster.log("Finished successfully")

@pytest.mark.timeout(6000, func_only=True)
def test_physical_PBM_T247(start_cluster,cluster):
    configure_failpoint_BackupCursorOpenConflictWithCheckpoint(cluster.connection)
    cluster.make_backup("physical")
    assert 'a checkpoint took place, retrying' in cluster.exec_pbm_cli("logs -sD -t0 -e backup").stdout
    Cluster.log("Finished successfully")
