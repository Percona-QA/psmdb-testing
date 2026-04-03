import pytest
import pymongo
import bson
import json
import os
import re

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
    return Cluster(config, mongod_extra_args="--enableEncryption --kmipServerName cosmian --kmipPort 5696 --kmipServerCAFile /etc/pykmip/ca.crt --kmipClientCertificateFile /etc/pykmip/mongod.pem")

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

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
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

@pytest.mark.timeout(300,func_only=True)
def test_physical_PBM_T197(start_cluster,cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many([{"data": i} for i in range(1000)])
    backup=cluster.make_backup("physical")
    result = cluster.exec_pbm_cli("describe-backup " + backup + " --out=json")
    assert result.rc == 0, result.stderr + result.stdout
    desc = json.loads(result.stdout)
    for rs in ["rscfg","rs1","rs2"]:
        kmip_id_storage = parse_storage_bson(backup,rs)["storage"]["options"]["encryption"]["kmip"]["keyId"]
        kmip_id_descr = parse_describe_backup(rs,desc)["security"]["kmip"]["keyIdentifier"]
        assert kmip_id_storage == kmip_id_descr
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == 1000
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1000
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300,func_only=True)
def test_incremental_PBM_T201(start_cluster,cluster):
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
        # retrieve kmip id from base backup's storage 
        kmip_id_storage = parse_storage_bson(base_backup,rs)["storage"]["options"]["encryption"]["kmip"]["keyId"]
        # retrieve kmip id from incermental backup's description
        kmip_id_descr = parse_describe_backup(rs,desc)["security"]["kmip"]["keyIdentifier"]
        assert kmip_id_storage == kmip_id_descr
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == 2000
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 2000
    Cluster.log("Finished successfully")
