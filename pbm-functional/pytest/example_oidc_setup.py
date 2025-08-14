import signal
from cluster import Cluster

#docker compose run -ti --rm test python3 -i example_oidc_setup.py

config = { "_id": "rs1", "members": [{"host": "rs101"}]}
pbm_mongodb_uri = 'mongodb://127.0.0.1:27017/?authSource=%24external&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s'
mongod_extra_args = ('--setParameter authenticationMechanisms=SCRAM-SHA-1,MONGODB-OIDC --setParameter \'' +
                    'oidcIdentityProviders=[ { ' +
                    '"issuer": "https://keycloak:8443/realms/test", ' +
                    '"clientId": "test-client", ' +
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
#cluster.setup_pbm()

signal.signal(signal.SIGINT,handler)
print("\nCluster is prepared and ready to use")
print("\nPress CTRL-C to destroy and exit")

