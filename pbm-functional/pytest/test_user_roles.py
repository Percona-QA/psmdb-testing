import pytest
import pymongo
import bson
import testinfra
import time
import os
import docker
import threading

from datetime import datetime
from cluster import Cluster


@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {
            "_id": "rscfg",
            "members": [{"host": "rscfg01"}, {"host": "rscfg02"}, {"host": "rscfg03"}],
        },
        "shards": [
            {
                "_id": "rs1",
                "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}],
            },
            {
                "_id": "rs2",
                "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}],
            },
        ],
    }

@pytest.fixture(scope="package")
def pbm_mongodb_uri():
    return 'mongodb://pbm_test:pbmpass_test1@127.0.0.1:27017/?authSource=admin'

@pytest.fixture(scope="package")
def newcluster(config, pbm_mongodb_uri):
    return Cluster(config, pbm_mongodb_uri=pbm_mongodb_uri)

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


@pytest.fixture(scope="function")
def start_cluster(cluster, newcluster, request):
    try:
        cluster.destroy()
        newcluster.destroy()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        try:
            cluster.destroy(cleanup_backups=True)
        except Exception as e:
            newcluster.destroy(cleanup_backups=True)


def check_user(client, db_name, username, expected_roles):
    try:
        db_query = client.db.command({"usersInfo": {"user": username, "db": db_name}})
        if db_query.get("ok") == 1 and len(db_query.get("users", [])) > 0:
            roles = {role['role'] for role in db_query['users'][0]['roles']}
            return roles == expected_roles
        else:
            return False
    except pymongo.errors.OperationFailure as e:
        return False

@pytest.mark.parametrize('restore_type',['part_bck','full_bck_part_rst_wo_user','full_bck_part_rst_user','full_bck'])
@pytest.mark.timeout(350, func_only=True)
def test_logical_PBM_T216(start_cluster, cluster, newcluster, restore_type):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    client_shard = pymongo.MongoClient("mongodb://root:root@rs101,rs102,rs103/?replicaSet=rs1")
    client.admin.command({"enableSharding": "test_db1", "primaryShard": "rs1"})
    client.admin.command({"enableSharding": "test_db2", "primaryShard": "rs2"})
    client.admin.command("shardCollection", "test_db1.test_coll11", key={"_id": "hashed"})
    client.admin.command('updateUser', 'pbm_test', pwd='pbmpass_test2')
    client.admin.command('createUser', 'admin_random_user1', pwd='test123', roles=[{'role':'readWrite','db':'admin'}, 'userAdminAnyDatabase', 'clusterAdmin'])
    client_shard.admin.command('createUser', 'admin_random_user2', pwd='test123', roles=[{'role':'readWrite','db':'admin'}, 'userAdminAnyDatabase', 'clusterAdmin'])
    client.test_db1.command('createUser', 'test_random_user1', pwd='test123', roles=[{'role':'readWrite','db':'test_db1'}, {'role':'clusterManager','db':'admin'}])
    client_shard.test_db1.command('createUser', 'test_random_user2', pwd='test123', roles=[{'role':'readWrite','db':'test_db1'}, {'role':'clusterManager','db':'admin'}])
    for i in range(10):
        client["test_db1"]["test_coll11"].insert_one({"key": i, "data": i})
        client["test_db2"]["test_coll21"].insert_one({"key": i, "data": i})
    backup_full = cluster.make_backup("logical")
    backup_partial = cluster.make_backup("logical --ns=test_db1.*,test_db2.*")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.5")
    client.admin.command('createUser', 'admin_random_user3', pwd='test123', roles=[{'role':'readWrite','db':'admin'}, 'userAdminAnyDatabase', 'clusterAdmin'])
    client_shard.admin.command('createUser', 'admin_random_user4', pwd='test123', roles=[{'role':'readWrite','db':'admin'}, 'userAdminAnyDatabase', 'clusterAdmin'])
    client.test_db1.command('createUser', 'test_random_user3', pwd='test123', roles=[{'role':'readWrite','db':'test_db1'}, {'role':'clusterManager','db':'admin'}])
    client_shard.test_db1.command('createUser', 'test_random_user4', pwd='test123', roles=[{'role':'readWrite','db':'test_db1'}, {'role':'clusterManager','db':'admin'}])
    for i in range(10):
        client["test_db1"]["test_coll11"].insert_one({"key": i+10, "data": i+10})
        client["test_db2"]["test_coll21"].insert_one({"key": i+10, "data": i+10})
    time.sleep(5)
    pitr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(pitr)
    pitr = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    client.drop_database("test_db1")
    client.drop_database("test_db2")
    client.admin.command("dropUser", "admin_random_user1")
    client_shard.admin.command("dropUser", "admin_random_user2")
    client.admin.command("dropUser", "admin_random_user3")
    client_shard.admin.command("dropUser", "admin_random_user4")
    client.test_db1.command("dropUser", "test_random_user1")
    client_shard.test_db1.command("dropUser", "test_random_user2")
    client.test_db1.command("dropUser", "test_random_user3")
    client_shard.test_db1.command("dropUser", "test_random_user4")

    # restoring users and roles from selective backup is not supported
    restore_commands = {
        'part_bck': " --base-snapshot=" + backup_partial + pitr,
        'full_bck_part_rst_wo_user': " --base-snapshot=" + backup_full + pitr + " --ns=test_db1.*,test_db2.*",
        'full_bck_part_rst_user': " --base-snapshot=" + backup_full + pitr + " --ns=test_db1.*,test_db2.* --with-users-and-roles",
        'full_bck': " --base-snapshot=" + backup_full + pitr
    }

    # re-create cluster with new PBM user for connection to check that restore and connection to DB are OK
    # despite the same user with different password is present in backup
    if restore_type == 'full_bck':
        cluster.destroy()
        newcluster.create()
        newcluster.setup_pbm()
        newcluster.check_pbm_status()
        newcluster.make_restore(restore_commands.get(restore_type), check_pbm_status=True)
    else:
        cluster.make_restore(restore_commands.get(restore_type), check_pbm_status=True)

    assert client["test_db1"]["test_coll11"].count_documents({}) == 20
    assert client["test_db1"].command("collstats", "test_coll11").get("sharded", False)
    assert client["test_db2"]["test_coll21"].count_documents({}) == 20
    assert client["test_db2"].command("collstats", "test_coll21").get("sharded", True) is False

    assert check_user(client, "admin", "admin_random_user1", {'readWrite', 'userAdminAnyDatabase', 'clusterAdmin'}) == \
                                                (restore_type == 'full_bck'), \
                                                f"Failed for {restore_type}: admin_random_user1 role mismatch"
    assert check_user(client_shard, "admin", "admin_random_user2", {'readWrite', 'userAdminAnyDatabase', 'clusterAdmin'}) == \
                                                (restore_type == 'full_bck'), \
                                                f"Failed for {restore_type}: admin_random_user2 role mismatch"
    assert check_user(client, "admin", "admin_random_user3", {'readWrite', 'userAdminAnyDatabase', 'clusterAdmin'}) == \
                                                (restore_type == 'full_bck'), \
                                                f"Failed for {restore_type}: admin_random_user3 role mismatch"
    assert check_user(client_shard, "admin", "admin_random_user4", {'readWrite', 'userAdminAnyDatabase', 'clusterAdmin'}) == \
                                                (restore_type == 'full_bck'), \
                                                f"Failed for {restore_type}: admin_random_user4 role mismatch"
    assert check_user(client, "test_db1", "test_random_user1", {'readWrite', 'clusterManager'}) == (restore_type not in \
                                                ['part_bck','full_bck_part_rst_wo_user']), \
                                                f"Failed for {restore_type}: test_random_user1 role mismatch"
    assert check_user(client_shard, "test_db1", "test_random_user2", {'readWrite', 'clusterManager'}) == (restore_type not in \
                                                ['part_bck','full_bck_part_rst_wo_user']), \
                                                f"Failed for {restore_type}: test_random_user2 role mismatch"
    # current limitation: option with-users-and-roles doesn't work with PITR
    assert check_user(client, "test_db1", "test_random_user3", {'readWrite', 'clusterManager'}) == (restore_type not in \
                                                ['part_bck','full_bck_part_rst_wo_user', 'full_bck_part_rst_user']), \
                                                f"Failed for {restore_type}: test_random_user3 role mismatch"
    assert check_user(client_shard, "test_db1", "test_random_user4", {'readWrite', 'clusterManager'}) == (restore_type not in \
                                                ['part_bck','full_bck_part_rst_wo_user', 'full_bck_part_rst_user']), \
                                                f"Failed for {restore_type}: test_random_user4 role mismatch"
    Cluster.log("Finished successfully")
