import signal
import requests
import testinfra
from cluster import Cluster

"""
example setup for pbm using OIDC with ENVIRONMENT:k8s
creates directory /var/run/secrets/kubernetes.io/serviceaccount
queries keycloack for access_token and put it into
/var/run/secrets/kubernetes.io/serviceaccount/token
to simulate k8s environment
important: file without newline in the end
to start:
docker compose run -ti --rm test python3 -i example_oidc_setup.py
"""

config = { "_id": "rs1", "members": [{"host": "rs101"}]}
pbm_mongodb_uri = 'mongodb://127.0.0.1:27017/?authSource=%24external&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s'
mongod_extra_args = ('--setParameter authenticationMechanisms=SCRAM-SHA-1,MONGODB-OIDC --setParameter \'' +
                    'oidcIdentityProviders=[ { ' +
                    '"issuer": "https://keycloak:8443/realms/test", ' +
                    '"clientId": "pbmclient", ' +
                    '"audience": "account", ' +
                    '"authNamePrefix": "keycloak", ' +
                    '"useAuthorizationClaim": false, ' +
                    '"supportsHumanFlows": false, ' +
                    '"principalName": "client_id" ' +
                    '} ]\'')
cluster = Cluster(config,pbm_mongodb_uri=pbm_mongodb_uri,mongod_extra_args=mongod_extra_args)

def handler(signum,frame):
    cluster.destroy()
    exit(0)

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
cluster.setup_pbm()
cluster.check_pbm_status()
signal.signal(signal.SIGINT,handler)
print("\nCluster is prepared and ready to use")
print("\nPress CTRL-C to destroy and exit")

