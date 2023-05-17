import pytest
import pymongo
import json
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
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        client.admin.command("shardCollection", "test.test2", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
#        cluster.destroy()

@pytest.mark.timeout(600,func_only=True)
def test_physical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)

    ##perform external backup
    result=cluster.exec_pbm_cli("backup -t external -o json")
    assert result.rc==0
    backup = json.loads(result.stdout)['name']
    Cluster.log("Backup name: " + backup)
    os.system("mkdir -p /backups/" + backup)
    timeout = time.time() + 300
    while True:
        status = cluster.get_status()
        Cluster.log(status['running'])
        if status['running']:
            if status['running']['status'] == "copyReady":
                break
        if time.time() > timeout:
            assert False
        time.sleep(1)
    result=cluster.exec_pbm_cli("describe-backup " + backup + " -o json")
    assert result.rc==0, result.stderr
    
    #modify data before copying
    time.sleep(60)
    pymongo.MongoClient(cluster.connection).drop_database('test')
    pymongo.MongoClient(cluster.connection)["test"]["test1"].insert_many(documents)

    description=json.loads(result.stdout)
    assert "replsets" in description
    for rs in description['replsets']:
        Cluster.log("Performing backup for RS " + rs['name'] + " source node: " + rs['node'].split(':')[0])
        n = testinfra.get_host("docker://" + rs['node'].split(':')[0])
        dir = "/backups/" + backup + "/" + rs['name'] + "/"
        os.system("mkdir -p " + dir)
        n.check_output("cp -rp /var/lib/mongo/* "  + dir)
    result = cluster.exec_pbm_cli("backup-finish " + backup + " -o json")
    assert result.rc==0, result.stderr
    #backup finished

    time.sleep(60)
    pymongo.MongoClient(cluster.connection)["test"]["test2"].insert_many(documents)

    ##perform restore
    cluster.stop_mongos()
    result=cluster.exec_pbm_cli("restore --external " + backup)
    assert result.rc==0, result.stderr
    Cluster.log(result.stdout)
    restore=result.stdout.split()[2]
    Cluster.log(restore)

    #restore configserver
    rsname=cluster.config['configserver']['_id']
    for node in cluster.config['configserver']['members']:
        n = testinfra.get_host("docker://" + node['host'])
        n.check_output("rm -rf /var/lib/mongo/*")
        files="/backups/" + backup + "/" + rsname + "/*"
        n.check_output("cp -rp "  + files + " /var/lib/mongo/")
        Cluster.log("Copying files " + files + " to host " + node['host'])

    #restore shards
    for shard in cluster.config['shards']:
        rsname=shard['_id']
        for node in shard['members']:
            n = testinfra.get_host("docker://" + node['host'])
            n.check_output("rm -rf /var/lib/mongo/*")
            files="/backups/" + backup + "/" + rsname + "/*"
            n.check_output("cp -rp "  + files + " /var/lib/mongo/")
            Cluster.log("Copying files " + files + " to host " + node['host'])

    #finalize restore
    result=cluster.exec_pbm_cli("restore-finish " + restore + " -c /etc/pbm.conf")
    assert result.rc==0, result.stderr
    Cluster.log(result.stdout)
    timeout = time.time() + 300
    while True:
        result=cluster.exec_pbm_cli("describe-restore " + restore + " -c /etc/pbm.conf -o json")
        assert result.rc==0, result.stderr
        status = json.loads(result.stdout)
        Cluster.log(status['status'])
        if status['status']=='done':
            Cluster.log(status)
            break
        if time.time() > timeout:
            assert False
        time.sleep(1)

    cluster.restart()
    cluster.restart_pbm_agents()
    cluster.make_resync()
    cluster.check_pbm_status()
    cluster.start_mongos()

    #check restore
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    assert pymongo.MongoClient(cluster.connection)["test"]["test1"].count_documents({}) == 0
    assert pymongo.MongoClient(cluster.connection)["test"]["test2"].count_documents({}) == 0
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
