import pytest
import pymongo
import time
import os
import docker
from datetime import datetime, timezone
from cluster import Cluster

"""
- Test creates custom roles and users in `admin` and `test_db1` databases
- After backup test drops users but keeps roles intact, this ensures that during a restore process, PBM can properly handle:
  - Restoring users correctly
  - Handling **pre-existing roles** without conflicts
"""

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {
            "_id": "rscfg",
            "members": [{"host": "rscfg01"}],
        },
        "shards": [
            {
                "_id": "rs1",
                "members": [{"host": "rs101"}],
            },
            {
                "_id": "rs2",
                "members": [{"host": "rs201"}],
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
        except Exception:
            newcluster.destroy(cleanup_backups=True)


def check_user(client, db_name, username, expected_roles, should_exist, restore_type=None):
    try:
        db_query = client[db_name].command({"usersInfo": {"user": username, "db": db_name}})
        user_exists = db_query.get("ok") == 1 and len(db_query.get("users", [])) > 0
        roles_match = user_exists and {role['role'] for role in db_query['users'][0]['roles']} == expected_roles
        if should_exist and not roles_match:
            raise AssertionError(f"{username} is missing or has wrong roles")
        if not should_exist and roles_match:
            raise AssertionError(f"{username} should NOT exist but does")
    except Exception as e:
        raise AssertionError(f"{e}")

@pytest.mark.parametrize('restore_type',['part_bck','full_bck_part_rst_wo_user','full_bck_part_rst_user1','full_bck_part_rst_user2','full_bck','full_pitr'])
@pytest.mark.timeout(350, func_only=True)
def test_logical_PBM_T216(start_cluster, cluster, newcluster, restore_type):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    client_shard = pymongo.MongoClient("mongodb://root:root@rs101,rs102,rs103/?replicaSet=rs1")
    for db_name, shard in [("administration", "rs1"), ("test_db1", "rs1"), ("test_db2", "rs2")]:
        client.admin.command({"enableSharding": db_name, "primaryShard": shard})
    client.admin.command("shardCollection", "test_db1.test_coll11", key={"_id": "hashed"})
    client.admin.command('updateUser', 'pbm_test', pwd='pbmpass_test2')
    role_configs = [
        {
            'db': 'admin',
            'role_name': 'customAdminRole',
            'role_name_sh': 'customAdminRoleSh',
            'privileges': [
                {'resource': {'db': 'admin', 'collection': 'system'}, 'actions': ['find', 'insert', 'update', 'remove']},
                {'resource': {'cluster': True}, 'actions': ['serverStatus', 'listDatabases', 'addShard', 'removeShard']}
            ],
            'roles': ['readWrite', 'userAdminAnyDatabase', 'clusterAdmin'],
            'user_name': 'admin_random_user1',
            'user_name_sh': 'admin_random_user2',
        },
        {
            'db': 'test_db1',
            'role_name': 'customTestDBRole',
            'role_name_sh': 'customTestDBRoleSh',
            'privileges': [
                {'resource': {'db': 'test_db1', 'collection': ''}, 'actions': ['find', 'insert', 'update', 'remove']}
            ],
            'roles': ['readWrite'],
            'user_name': 'test_random_user1',
            'user_name_sh': 'test_random_user2',
        },
        {
            'db': 'administration',
            'role_name': 'customAdministrationDBRole',
            'role_name_sh': 'customAdministrationDBRoleSh',
            'privileges': [
                {'resource': {'db': 'administration', 'collection': ''}, 'actions': ['find', 'insert', 'update', 'remove']}
            ],
            'roles': ['readWrite'],
            'user_name': 'administration_random_user1',
            'user_name_sh': 'administration_random_user2',
        },
    ]
    for config in role_configs:
        client[config['db']].command('createRole', config['role_name'], privileges=config['privileges'], roles=config['roles'])
        client_shard[config['db']].command('createRole', config['role_name_sh'], privileges=config['privileges'], roles=config['roles'])
        client[config['db']].command('createUser', config['user_name'], pwd='test123', roles=[{'role': config['role_name'], 'db': config['db']}])
        client_shard[config['db']].command('createUser', config['user_name_sh'], pwd='test123', roles=[{'role': config['role_name_sh'], 'db': config['db']}])
    for i in range(10):
        client["test_db1"]["test_coll11"].insert_one({"key": i, "data": i})
        client["test_db2"]["test_coll21"].insert_one({"key": i, "data": i})
    backup_full = cluster.make_backup("logical")
    backup_partial = cluster.make_backup("logical --ns=administration.*,test_db1.*,test_db2.*")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    builtin_user_configs = [
        {
            'db': 'admin',
            'user_name': 'admin_random_user3',
            'user_name_sh': 'admin_random_user4',
            'roles': [{'role': 'readWrite', 'db': 'admin'}, 'userAdminAnyDatabase', 'clusterAdmin'],
        },
        {
            'db': 'test_db1',
            'user_name': 'test_random_user3',
            'user_name_sh': 'test_random_user4',
            'roles': [{'role': 'readWrite', 'db': 'test_db1'}, {'role': 'clusterManager', 'db': 'admin'}],
        },
        {
            'db': 'administration',
            'user_name': 'administration_random_user3',
            'user_name_sh': 'administration_random_user4',
            'roles': [{'role': 'readWrite', 'db': 'administration'}, {'role': 'clusterManager', 'db': 'admin'}],
        },
    ]
    for config in builtin_user_configs:
        client[config['db']].command('createUser', config['user_name'], pwd='test123', roles=config['roles'])
        client_shard[config['db']].command('createUser', config['user_name_sh'], pwd='test123', roles=config['roles'])
    for i in range(10):
        client["test_db1"]["test_coll11"].insert_one({"key": i+10, "data": i+10})
        client["test_db2"]["test_coll21"].insert_one({"key": i+10, "data": i+10})
    time.sleep(5)
    pitr = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(pitr)
    pitr = " --time=" + pitr
    Cluster.log("Time for PITR is: " + pitr)
    client.drop_database("test_db1")
    client.drop_database("test_db2")
    client.drop_database("administration")
    users_to_drop = {
        client: {
            "admin": ["admin_random_user1", "admin_random_user3"],
            "test_db1": ["test_random_user1", "test_random_user3"],
            "administration": ["administration_random_user1", "administration_random_user3"],
        },
        client_shard: {
            "admin": ["admin_random_user2", "admin_random_user4"],
            "test_db1": ["test_random_user2", "test_random_user4"],
            "administration": ["administration_random_user2", "administration_random_user4"],
        }
    }
    for db_client, dbs in users_to_drop.items():
        for db_name, users in dbs.items():
            db = getattr(db_client, db_name)
            for user in users:
                db.command("dropUser", user)
    # restoring users and roles from selective backup is not supported
    restore_commands = {
        'part_bck': " --base-snapshot=" + backup_partial + pitr,
        'full_bck_part_rst_wo_user': " --base-snapshot=" + backup_full + pitr + " --ns=administration.*,test_db1.*,test_db2.*",
        'full_bck_part_rst_user1': " --base-snapshot=" + backup_full + pitr + " --ns=administration.*,test_db1.*,test_db2.* --with-users-and-roles",
        'full_bck_part_rst_user2': " --base-snapshot=" + backup_full + pitr + " --ns=administration.* --with-users-and-roles",
        'full_bck': " --base-snapshot=" + backup_full + pitr,
        'full_pitr': " --base-snapshot=" + backup_full + pitr
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
    if restore_type != 'full_bck_part_rst_user2':
        assert client["test_db1"]["test_coll11"].count_documents({}) == 20
        assert client["test_db1"].command("collstats", "test_coll11").get("sharded", False)
        assert client["test_db2"]["test_coll21"].count_documents({}) == 20
        assert client["test_db2"].command("collstats", "test_coll21").get("sharded", True) is False
    failures = []
    def run_check(*args, **kwargs):
        try:
            check_user(*args, **kwargs)
        except AssertionError as e:
            failures.append(str(e))
    # Lookup table: which users should exist for each restore type
    user_expectations = {
        'part_bck': {
            'admin_db_users_bcp': False,
            'admin_db_users_pitr': False,
            'test_db_users_bcp': False,
            'test_db_users_pitr': False,
            'administration_db_users_bcp': False,
            'administration_db_users_pitr': False,
        },
        'full_bck_part_rst_wo_user': {
            'admin_db_users_bcp': False,
            'admin_db_users_pitr': False,
            'test_db_users_bcp': False,
            'test_db_users_pitr': False,
            'administration_db_users_bcp': False,
            'administration_db_users_pitr': False,
        },
        'full_bck_part_rst_user1': {
            'admin_db_users_bcp': False,
            'admin_db_users_pitr': False,
            'test_db_users_bcp': True,
            'test_db_users_pitr': False,            # PITR limitation: not in backup
            'administration_db_users_bcp': True,
            'administration_db_users_pitr': False,  # PITR limitation: not in backup
        },
        'full_bck_part_rst_user2': {
            'admin_db_users_bcp': False,
            'admin_db_users_pitr': False,
            'test_db_users_bcp': False,
            'test_db_users_pitr': False,
            'administration_db_users_bcp': True,
            'administration_db_users_pitr': False,  # PITR limitation: not in backup
        },
        'full_bck': {
            'admin_db_users_bcp': True,
            'admin_db_users_pitr': True,
            'test_db_users_bcp': True,
            'test_db_users_pitr': True,
            'administration_db_users_bcp': True,
            'administration_db_users_pitr': True,
        },
        'full_pitr': {
            'admin_db_users_bcp': True,
            'admin_db_users_pitr': True,
            'test_db_users_bcp': True,
            'test_db_users_pitr': True,
            'administration_db_users_bcp': True,
            'administration_db_users_pitr': True,
        },
    }
    expectations = user_expectations[restore_type]
    # Admin database users
    run_check(client, "admin", "admin_random_user1", {'customAdminRole'}, expectations['admin_db_users_bcp'], restore_type)
    run_check(client_shard, "admin", "admin_random_user2", {'customAdminRoleSh'}, expectations['admin_db_users_bcp'], restore_type)
    run_check(client, "admin", "admin_random_user3", {'readWrite', 'userAdminAnyDatabase', 'clusterAdmin'}, expectations['admin_db_users_pitr'], restore_type)
    run_check(client_shard, "admin", "admin_random_user4", {'readWrite', 'userAdminAnyDatabase', 'clusterAdmin'}, expectations['admin_db_users_pitr'], restore_type)
    # Test DB users
    run_check(client, "test_db1", "test_random_user1", {'customTestDBRole'}, expectations['test_db_users_bcp'], restore_type)
    run_check(client_shard, "test_db1", "test_random_user2", {'customTestDBRoleSh'}, expectations['test_db_users_bcp'], restore_type)
    run_check(client, "test_db1", "test_random_user3", {'readWrite', 'clusterManager'}, expectations['test_db_users_pitr'], restore_type)
    run_check(client_shard, "test_db1", "test_random_user4", {'readWrite', 'clusterManager'}, expectations['test_db_users_pitr'], restore_type)
    # Administration DB users
    run_check(client, "administration", "administration_random_user1", {'customAdministrationDBRole'}, expectations['administration_db_users_bcp'], restore_type)
    run_check(client_shard, "administration", "administration_random_user2", {'customAdministrationDBRoleSh'}, expectations['administration_db_users_bcp'], restore_type)
    run_check(client, "administration", "administration_random_user3", {'readWrite', 'clusterManager'}, expectations['administration_db_users_pitr'], restore_type)
    run_check(client_shard, "administration", "administration_random_user4", {'readWrite', 'clusterManager'}, expectations['administration_db_users_pitr'], restore_type)
    if failures:
        raise AssertionError("User mismatch: \n" + " \n".join(failures))
    else:
        Cluster.log("Finished successfully")