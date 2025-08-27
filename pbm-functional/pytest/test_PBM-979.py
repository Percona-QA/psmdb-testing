import pytest
import pymongo
import time

from datetime import datetime
from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [
        {"host": "rs101"},
        {"host": "rs102", "priority": 2 },
        {"host": "rs103", "hidden": True, "priority": 0, "votes": 0},
        {"host": "rs104", "secondaryDelaySecs": 1, "priority": 0, "votes": 0, "buildIndexes": False },
        {"host": "rs105", "arbiterOnly": True }
    ]}


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        time.sleep(5) # wait for delayed node
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical_PBM_T233(start_cluster,cluster):
    time.sleep(5) # wait for delayed node
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("logical")
    #check if the backup was taken from the hidden node
    logs=cluster.exec_pbm_cli("logs -n rs1/rs103:27017 -e backup -o json").stdout
    assert backup in logs
    Cluster.log("Logs from hidden node:\n" + logs)
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup)
    time.sleep(5) # wait for delayed node
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_logical_pitr_PBM_T263(start_cluster,cluster):
    time.sleep(5) # wait for delayed node
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("logical")
    #check if the backup was taken from the hidden node
    logs=cluster.exec_pbm_cli("logs -n rs1/rs103:27017 -e backup -o json").stdout
    assert backup in logs
    Cluster.log("Logs from hidden node:\n" + logs)
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    pymongo.MongoClient(cluster.connection)["test"]["pitr"].insert_many(documents)
    time.sleep(10)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    backup="--time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    time.sleep(10)
    cluster.disable_pitr()
    time.sleep(10)
    pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    pymongo.MongoClient(cluster.connection)["test"]["pitr"].delete_many({})
    cluster.make_restore(backup)
    time.sleep(5) # wait for delayed node
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["pitr"].count_documents({}) == len(documents)
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_physical_PBM_T195(start_cluster,cluster):
    time.sleep(5) # wait for delayed node
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    #check if the backup was taken from the hidden node
    logs=cluster.exec_pbm_cli("logs -n rs1/rs103:27017 -e backup -o json").stdout
    assert backup in logs
    Cluster.log("Logs from hidden node:\n" + logs)
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,restart_cluster=True)
    time.sleep(5) # wait for delayed node
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    Cluster.log("Cluster config: \n" + str(cluster.config))
    rs_config = pymongo.MongoClient(cluster.connection).admin.command('replSetGetConfig')['config']
    Cluster.log("RS config after restore: \n" + str(rs_config))
    for member in cluster.config['members']:
        index = cluster.config['members'].index(member)
        assert member['host'] in rs_config['members'][index]['host']
        assert 'priority' not in member or member['priority'] == rs_config['members'][index]['priority']
        assert 'hidden' not in member or member['hidden'] == rs_config['members'][index]['hidden']
        assert 'votes' not in member or member['votes'] == rs_config['members'][index]['votes']
        assert 'secondaryDelaySecs' not in member or member['secondaryDelaySecs'] == rs_config['members'][index]['secondaryDelaySecs']
        assert 'slaveDelay' not in member or member['slaveDelay'] == rs_config['members'][index]['slaveDelay']
        assert 'buildIndexes' not in member or member['buildIndexes'] == rs_config['members'][index]['buildIndexes']
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_incremental_PBM_T234(start_cluster,cluster):
    time.sleep(5)
    cluster.check_pbm_status()
    init_backup=cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("incremental")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    logs=cluster.exec_pbm_cli("logs -t 200 -n rs1/rs103:27017 -e backup -o json").stdout
    Cluster.log("Logs from hidden node:\n" + logs)
    assert init_backup in logs
    assert backup in logs
    cluster.make_restore(backup,restart_cluster=True)
    time.sleep(5)
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    Cluster.log("Cluster config: \n" + str(cluster.config))
    rs_config = pymongo.MongoClient(cluster.connection).admin.command('replSetGetConfig')['config']
    Cluster.log("RS config after restore: \n" + str(rs_config))
    for member in cluster.config['members']:
        index = cluster.config['members'].index(member)
        assert member['host'] in rs_config['members'][index]['host']
        assert 'priority' not in member or member['priority'] == rs_config['members'][index]['priority']
        assert 'hidden' not in member or member['hidden'] == rs_config['members'][index]['hidden']
        assert 'votes' not in member or member['votes'] == rs_config['members'][index]['votes']
        assert 'secondaryDelaySecs' not in member or member['secondaryDelaySecs'] == rs_config['members'][index]['secondaryDelaySecs']
        assert 'slaveDelay' not in member or member['slaveDelay'] == rs_config['members'][index]['slaveDelay']
        assert 'buildIndexes' not in member or member['buildIndexes'] == rs_config['members'][index]['buildIndexes']
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_external_PBM_T240(start_cluster,cluster):
    time.sleep(5)
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup = cluster.external_backup_start()
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.external_backup_copy(backup)
    cluster.external_backup_finish(backup)
    time.sleep(10)
    restore=cluster.external_restore_start()
    cluster.external_restore_copy(backup)
    cluster.external_restore_finish(restore)
    time.sleep(5)
    cluster.check_pbm_status()
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    Cluster.log("Cluster config: \n" + str(cluster.config))
    rs_config = pymongo.MongoClient(cluster.connection).admin.command('replSetGetConfig')['config']
    Cluster.log("RS config after restore: \n" + str(rs_config))
    for member in cluster.config['members']:
        index = cluster.config['members'].index(member)
        assert member['host'] in rs_config['members'][index]['host']
        assert 'priority' not in member or member['priority'] == rs_config['members'][index]['priority']
        assert 'hidden' not in member or member['hidden'] == rs_config['members'][index]['hidden']
        assert 'votes' not in member or member['votes'] == rs_config['members'][index]['votes']
        assert 'secondaryDelaySecs' not in member or member['secondaryDelaySecs'] == rs_config['members'][index]['secondaryDelaySecs']
        assert 'slaveDelay' not in member or member['slaveDelay'] == rs_config['members'][index]['slaveDelay']
        assert 'buildIndexes' not in member or member['buildIndexes'] == rs_config['members'][index]['buildIndexes']
    Cluster.log("Finished successfully")
