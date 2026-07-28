import pytest
import pymongo
import docker
import requests
import testinfra

from cluster import Cluster

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="package")
def pbm_mongodb_uri():
    return 'mongodb://127.0.0.1:27017/?authSource=%24external&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s'

@pytest.fixture(scope="package")
def mongod_extra_args():
    args = ('--setParameter authenticationMechanisms=SCRAM-SHA-1,MONGODB-OIDC --setParameter \'' +
                    'oidcIdentityProviders=[ { ' +
                    '"issuer": "https://keycloak:8443/realms/test", ' +
                    '"clientId": "pbmclient", ' +
                    '"audience": "account", ' +
                    '"authNamePrefix": "keycloak", ' +
                    '"useAuthorizationClaim": false, ' +
                    '"supportsHumanFlows": false, ' +
                    '"principalName": "client_id" ' +
                    '} ]\'')
    return args

@pytest.fixture(scope="package")
def cluster(config,pbm_mongodb_uri,mongod_extra_args):
    return Cluster(config, pbm_mongodb_uri=pbm_mongodb_uri, mongod_extra_args=mongod_extra_args)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        response=requests.post(
            'https://keycloak:8443/realms/test/protocol/openid-connect/token',
            data = {
                "grant_type": "client_credentials",
                "client_id": "pbmclient",
                "client_secret": "pbm-secret"
            },
            verify=False
        )
        token = response.json()["access_token"]
        print(token)
        token_dir="/var/run/secrets/kubernetes.io/serviceaccount"
        token_path="/var/run/secrets/kubernetes.io/serviceaccount/token"
        host = testinfra.get_host("docker://rs101")
        host.check_output(f"mkdir -p {token_dir}")
        host.check_output(f"printf %s {token} > {token_path}")
        # restart agents so they authenticate with the now-written OIDC token
        cluster.restart_pbm_agents()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical_PBM_T335(start_cluster,cluster):
    cluster.check_pbm_status()
    pymongo.MongoClient(cluster.connection)["test"]["test"].insert_one({})
    backup=cluster.make_backup("logical")
    result=pymongo.MongoClient(cluster.connection)["test"]["test"].delete_many({})
    assert int(result.deleted_count) == 1
    cluster.make_restore(backup,check_pbm_status=True)
    assert pymongo.MongoClient(cluster.connection)["test"]["test"].count_documents({}) == 1
    Cluster.log("Finished successfully")

