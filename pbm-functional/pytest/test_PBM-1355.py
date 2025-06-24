import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker

from datetime import datetime
from cluster import Cluster
from packaging import version


@pytest.fixture(scope="package")
def mongod_version():
    return docker.from_env().containers.run(
                    image='replica_member/local',
                    remove=True,
                    command='bash -c \'mongod --version | head -n1 | sed "s/db version v//"\''
          ).decode("utf-8", errors="replace")

@pytest.fixture(scope="package")
def config(mongod_version):
    if version.parse(mongod_version) < version.parse("8.0.0"):
        pytest.skip("Unsupported version for config shards")
    else:
        return { "mongos": "mongos",
                 "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"},{"host": "rscfg02"},{"host": "rscfg03" }]},
                 "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]}
                      ]}

@pytest.fixture(scope="package")
def newconfig():
    return { "mongos": "mongos",
             "configserver":
                   {"_id": "newrscfg", "members": [{"host":"newrscfg01"},{"host": "newrscfg02"},{"host": "newrscfg03" }]},
             "shards":[
                   {"_id": "newrs1", "members": [{"host":"newrs101"},{"host": "newrs102"},{"host": "newrs103" }]}
           ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="package")
def newcluster(newconfig):
    return Cluster(newconfig)

@pytest.fixture(scope="function")
def start_cluster(cluster,newcluster,request):
    try:
        newcluster.destroy()
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        client=pymongo.MongoClient(cluster.connection)
        Cluster.log(client.admin.command({'transitionFromDedicatedConfigServer': 1}))
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        cluster.setup_pbm()
        result = cluster.exec_pbm_cli("config --set storage.type=filesystem --set storage.filesystem.path=/backups "
                                    "--set backup.compression=none --wait")
        assert result.rc == 0
        Cluster.log("Setup PBM with fs storage:\n" + result.stdout)
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()
        newcluster.destroy()

@pytest.mark.timeout(900,func_only=True)
@pytest.mark.parametrize('backup_type',['logical','physical'])
@pytest.mark.parametrize('restore_type',['base','pitr'])
def test_general_PBM_T257(start_cluster,cluster,backup_type,restore_type):
    cluster.check_pbm_status()
    client=pymongo.MongoClient(cluster.connection)
    for i in range(600):
        client['test']['test'].insert_one({"doc":i})

    backup=cluster.make_backup(backup_type)
    restart = True if backup_type == 'physical' else False
    if restore_type == 'base':
        pymongo.MongoClient(cluster.connection).drop_database('test')
        cluster.make_restore(backup,restart_cluster=restart,check_pbm_status=True,make_resync=False)
        assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 600
        assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)
    else:
        cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
        for i in range(600):
            client['test']['test'].insert_one({"doc":i})
            time.sleep(0.1)
        time.sleep(60)
        pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        Cluster.log("Time for PITR is: " + pitr)
        pitr_backup="--time=" + pitr
        time.sleep(60)
        pymongo.MongoClient(cluster.connection).drop_database('test')
        cluster.make_restore(pitr_backup,restart_cluster=restart,check_pbm_status=True,make_resync=False)
        assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1200
        assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)

@pytest.mark.timeout(900,func_only=True)
@pytest.mark.parametrize('backup_type',['logical','physical'])
def test_remap_PBM_T265(start_cluster,cluster,newcluster,backup_type):
    cluster.check_pbm_status()
    client=pymongo.MongoClient(cluster.connection)
    for i in range(600):
        client['test']['test'].insert_one({"doc":i})

    backup=cluster.make_backup(backup_type)
    backup = backup + ' --replset-remapping="newrs1=rs1,newrscfg=rscfg"'
    cluster.destroy()

    newcluster.create()
    client=pymongo.MongoClient(newcluster.connection)
    Cluster.log(client.admin.command({'transitionFromDedicatedConfigServer': 1}))
    newcluster.setup_pbm()
    result = newcluster.exec_pbm_cli("config --set storage.type=filesystem "
            "--set storage.filesystem.path=/backups --set backup.compression=none --wait")
    assert result.rc == 0

    restart = True if backup_type == 'physical' else False
    newcluster.make_restore(backup,restart_cluster=restart,check_pbm_status=True,make_resync=False)
    assert pymongo.MongoClient(newcluster.connection)["test"]["test"].count_documents({}) == 600
    assert pymongo.MongoClient(newcluster.connection)["test"].command("collstats", "test").get("sharded", False)

@pytest.mark.timeout(900,func_only=True)
def test_incremental_PBM_T258(start_cluster,cluster):
    cluster.check_pbm_status()
    client=pymongo.MongoClient(cluster.connection)
    for i in range(600):
        client['test']['test'].insert_one({"doc":i})
    cluster.make_backup("incremental --base")
    for i in range(600):
        client['test']['test'].insert_one({"doc":i})
        time.sleep(0.1)
    time.sleep(10)
    backup=cluster.make_backup("incremental")
    pymongo.MongoClient(cluster.connection).drop_database('test')
    cluster.make_restore(backup,restart_cluster=True,check_pbm_status=True,make_resync=False)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1200
    assert pymongo.MongoClient(cluster.connection)["test"].command("collstats", "test").get("sharded", False)

@pytest.mark.parametrize('command',['config --force-resync','backup'])
def test_disabled_cli_PBM_T260(start_cluster,cluster,command):
    cluster.check_pbm_status()
    result = cluster.exec_pbm_cli(command + ' --wait')
    assert result.rc == 0, result.stderr
    Cluster.log(result.stdout)


@pytest.mark.timeout(900,func_only=True)
@pytest.mark.parametrize('restore_ns',['sharded','unsharded'])
@pytest.mark.parametrize('restore_type',['base','pitr'])
@pytest.mark.parametrize('backup_type',['full','part'])
def test_load_selective_PBM_T259(start_cluster,cluster,backup_type,restore_type,restore_ns):
    cluster.check_pbm_status()
    client=pymongo.MongoClient(cluster.connection)
    for i in range(600):
        client['test']['test'].insert_one({"doc":i})
        client['test']['test1'].insert_one({"doc":i})

    if restore_ns == 'sharded':
        collection = 'test'
        empty_collection = 'test1'
    else:
        collection = 'test1'
        empty_collection = 'test'

    backup = cluster.make_backup("logical")

    if backup_type == 'part':
       backup = cluster.make_backup("logical --ns=test." + collection)
       restore = backup
    else:
       restore = backup + ' --ns=test.' + collection

    if restore_type == 'base':
        pymongo.MongoClient(cluster.connection).drop_database('test')
        cluster.make_restore(restore,restart_cluster=False,check_pbm_status=True,make_resync=False)
        assert pymongo.MongoClient(cluster.connection)["test"][collection].count_documents({}) == 600
        assert pymongo.MongoClient(cluster.connection)["test"][empty_collection].count_documents({}) == 0
    else:
        cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
        for i in range(600):
            client['test']['test'].insert_one({"doc":i})
            client['test']['test1'].insert_one({"doc":i})
            time.sleep(0.1)
        time.sleep(10)
        pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        Cluster.log("Time for PITR is: " + pitr)
        cluster.disable_pitr(pitr)
        restore = " --base-snapshot=" + restore + " --time=" + pitr
        pymongo.MongoClient(cluster.connection).drop_database('test')
        cluster.make_restore(restore,restart_cluster=False,check_pbm_status=True,make_resync=False)
        assert pymongo.MongoClient(cluster.connection)["test"][collection].count_documents({}) == 1200
        assert pymongo.MongoClient(cluster.connection)["test"][empty_collection].count_documents({}) == 0
