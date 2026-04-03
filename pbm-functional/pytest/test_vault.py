import pytest
import pymongo
import bson
import json
import os
import re
import requests
import time

from cluster import Cluster

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
    return Cluster(config, mongod_extra_args="--enableEncryption --vaultServerName vault --vaultPort 8200 --vaultTokenFile /etc/vault/token --vaultSecret secret/data/mongo --vaultDisableTLSForTesting")

def increase_max_versions_vault():
    headers = {
        "X-Vault-Token": "vaulttoken",
        "Content-Type": "application/json"
    }

    payload = {
        "max_versions": 1024
    }

    result = requests.post(
        "http://vault:8200/v1/secret/config",
        headers=headers,
        data=json.dumps(payload)
    )

    Cluster.log(result.text)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        increase_max_versions_vault()
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm("/etc/pbm-fs.conf")
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def parse_storage_bson(backup,rs):
    dir = '/backups/' + backup + '/' + rs
    #need to find storage.bson by pattern because it can be named like storage.bson.0-190
    pattern = re.compile(r'storage\.bson')
    files = [f for f in os.listdir(dir) if pattern.match(f)]
    file = dir + "/" + files[0]
    with open(file, "rb") as f:
        data= f.read()
        docs = bson.decode_all(data)
        # storage.bson is always one-liner
        Cluster.log(docs[0])
        return docs[0]

def parse_describe_backup(rs,descr):
    result = next(filter(lambda x: x["name"] == rs, descr["replsets"]), None)
    Cluster.log(result)
    return result


@pytest.mark.timeout(300,func_only=True)
def test_physical_PBM_T196(start_cluster,cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many([{"data": i} for i in range(1000)])
    backup=cluster.make_backup("physical")
    result = cluster.exec_pbm_cli("describe-backup " + backup + " --out=json")
    assert result.rc == 0, result.stderr + result.stdout
    desc = json.loads(result.stdout)
    for rs in ["rscfg","rs1","rs2"]:
        vault_id_storage = parse_storage_bson(backup,rs)["storage"]["options"]["encryption"]["vault"]["version"]
        vault_id_descr = parse_describe_backup(rs,desc)["security"]["vault"]["secretVersion"]
        assert parse_describe_backup(rs,desc)["security"]["vault"]["secret"] == "secret/data/mongo"
        assert str(vault_id_storage) == str(vault_id_descr)
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == 1000
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1000
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_incremental_PBM_T200(start_cluster,cluster):
    cluster.check_pbm_status()
    client=pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many([{"data": i} for i in range(1000)])
    base_backup = cluster.make_backup("incremental --base")
    client["test"]["test"].insert_many([{"data": i} for i in range(1000)])
    backup=cluster.make_backup("incremental")
    result = cluster.exec_pbm_cli("describe-backup " + backup + " --out=json")
    assert result.rc == 0, result.stderr + result.stdout
    desc = json.loads(result.stdout)
    for rs in ["rscfg","rs1","rs2"]:
        # retrieve vault secret version from base backup's storage 
        vault_id_storage = parse_storage_bson(base_backup,rs)["storage"]["options"]["encryption"]["vault"]["version"]
        # retrieve vault secret version from incermental backup's description
        vault_id_descr = parse_describe_backup(rs,desc)["security"]["vault"]["secretVersion"]
        assert parse_describe_backup(rs,desc)["security"]["vault"]["secret"] == "secret/data/mongo"
        assert str(vault_id_storage) == str(vault_id_descr)
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == 2000
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 2000
    Cluster.log("Finished successfully")

@pytest.mark.timeout(600,func_only=True)
def test_external_PBM_T239(start_cluster,cluster):
    cluster.setup_pbm()
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many([{"data": i} for i in range(1000)])
    backup = cluster.external_backup_start()
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == 1000
    cluster.external_backup_copy(backup)
    cluster.external_backup_finish(backup)
    time.sleep(10)

    restore=cluster.external_restore_start()
    cluster.external_restore_copy(backup)
    cluster.external_restore_finish(restore)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1000
    Cluster.log("Finished successfully")
