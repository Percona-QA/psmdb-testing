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
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"}]}
                      ]}

@pytest.fixture(scope="package")
def pbm_mongodb_uri():
    return 'mongodb://cn%3Dpbm%2Cou%3Dusers%2Cdc%3Dexample%2Cdc%3Dorg:pbmpass@127.0.0.1:27017/?authSource=%24external&authMechanism=PLAIN'

@pytest.fixture(scope="package")
def mongod_extra_args():
    return (" --ldapServers=ldap:1389 --ldapTransportSecurity=none" +
            " --ldapAuthzQueryTemplate='dc=example,dc=org??sub?(&(objectClass=groupOfNames)(member={PROVIDED_USER}))'" +
            " --setParameter=authenticationMechanisms=SCRAM-SHA-1,PLAIN")

@pytest.fixture(scope="package")
def cluster(config,pbm_mongodb_uri,mongod_extra_args):
    return Cluster(config, pbm_mongodb_uri=pbm_mongodb_uri, mongod_extra_args=mongod_extra_args)

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
        cluster.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_logical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("logical")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    print("\nFinished successfully\n")

@pytest.mark.timeout(300,func_only=True)
def test_physical(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_many(documents)
    backup=cluster.make_backup("physical")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == len(documents)
    cluster.make_restore(backup,restart_cluster=True, make_resync=True, check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == len(documents)
    print("\nFinished successfully\n")

