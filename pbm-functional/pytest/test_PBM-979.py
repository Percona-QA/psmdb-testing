import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker

from datetime import datetime
from cluster import Cluster

documents=[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def mongod_version():
    return docker.from_env().containers.run(
                    image='replica_member/local',
                    remove=True,
                    command='mongod --version'
          ).decode("utf-8", errors="replace")

@pytest.fixture(scope="package")
def config(mongod_version):
    if "4.2" in mongod_version or "4.4" in mongod_version:
        return { "_id": "rs1", "members": [
            {"host": "rs101"},
            {"host": "rs102", "priority": 2 },
            {"host": "rs103", "hidden": True, "priority": 0, "votes": 0},
            {"host": "rs104", "slaveDelay": 1, "priority": 0, "votes": 0, "buildIndexes": False },
            {"host": "rs105", "arbiterOnly": True }
        ]}
    else:
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
        cluster.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_logical(start_cluster,cluster):
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

@pytest.mark.timeout(300,func_only=True)
def test_physical(start_cluster,cluster):
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
def test_incremental(start_cluster,cluster):
    time.sleep(5)
    cluster.check_pbm_status()
    init_backup=cluster.make_backup("incremental --base")
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("incremental")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    logs=cluster.exec_pbm_cli("logs -n rs1/rs103:27017 -e backup -o json").stdout
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

