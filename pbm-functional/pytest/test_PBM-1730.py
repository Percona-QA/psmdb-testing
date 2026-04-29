import pytest
import pymongo

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]},
                            {"_id": "rs2", "members": [{"host":"rs201"}]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config,mongod_extra_args='--setParameter enableTestCommands=1')

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        for i in range(100):
             client["test"]["test"].insert_one({"data": i})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def configure_failpoint_configsvrBalancerStop(connection,timeout):
    client = pymongo.MongoClient(connection)
    data = {'blockConnection': True, 'blockTimeMS': timeout, 'failCommands': ['_configsvrBalancerStop']}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': 'alwaysOn', 'data': data})
    Cluster.log(result)

@pytest.mark.timeout(300, func_only=True)
def test_restore_logical_balancer_timeout_PBM_T331(start_cluster,cluster):
    """
    Verify that logical restore fails when it cannot stop the balancer
    """
    backup = cluster.make_backup('logical')
    result = cluster.exec_pbm_cli('config --set restore.timeouts.balancerStop=10')
    assert result.rc == 0, result.stdout + result.stderr
    configure_failpoint_configsvrBalancerStop('mongodb://root:root@rscfg01:27017/?authSource=admin',11000)
    result = cluster.exec_pbm_cli('restore -y ' + backup)
    Cluster.log(result.stderr)
    assert result.rc == 1, result.stdout + result.stderr
    assert 'set balancer off: run mongo command: (MaxTimeMSExpired) operation exceeded time limit' in result.stderr
    Cluster.log("Finished succesfully")

@pytest.mark.timeout(300, func_only=True)
def test_restore_physical_balancer_timeout_PBM_T332(start_cluster,cluster):
    """
    Verify that physical restore ignores the timeout for balancer
    """
    backup = cluster.make_backup('physical')
    result = cluster.exec_pbm_cli('config --set restore.timeouts.balancerStop=10')
    assert result.rc == 0, result.stdout + result.stderr
    configure_failpoint_configsvrBalancerStop('mongodb://root:root@rscfg01:27017/?authSource=admin',11000)
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    Cluster.log("Finished succesfully")
