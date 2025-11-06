(function() {
    'use strict';

    var conn = MongoRunner.runMongod();

    var admin = conn.getDB("admin");

    admin.createRole({
        role: "keycloak/Everyone", privileges: [],
        roles: [ "readWriteAnyDatabase"]
    });

    admin.logout()

    var ext = conn.getDB("$external");

    ext.createUser({
        user: "keycloak/pbmclient",
        roles: [ { role: "keycloak/Everyone", db: "admin" } ]
    });

    ext.logout()

    MongoRunner.stopMongod(conn);

    var oidcProviders = JSON.stringify(
        [
                {
                    issuer: "https://keycloak:8443/realms/test",
                    clientId: "pbmclient",
                    audience: "account",
                    authNamePrefix: "keycloak",
                    useAuthorizationClaim: false,
                    supportsHumanFlows: false,
                    principalName: "client_id"
                }
        ]
    );

    // test command line parameters related to OIDC authorization
    conn = MongoRunner.runMongod({
        restart: conn,
        auth: '',
        setParameter: {authenticationMechanisms: 'SCRAM-SHA-1,MONGODB-OIDC', oidcIdentityProviders: oidcProviders},
        noCleanData: true
    });

    assert(conn, "Cannot start mongod instance");

    const token_raw = "/tmp/oidc_token.txt"

    let token_command = runProgram("bash", "-lc",
      "curl --silent --show-error " +
      "-X POST 'https://keycloak:8443/realms/test/protocol/openid-connect/token' " +
      "-d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' " +
      "| jq -r .access_token > " + token_raw
    );

    const token = cat(token_raw).trim();

    var clientConnect = function(conn) {
        const exitCode = runMongoProgram("/percona-server-mongodb/mongo",
                                         "--quiet",
                                         "--port", conn.port,
                                         "--authenticationDatabase", '$external',
                                         "--authenticationMechanism", "MONGODB-OIDC",
                                         "--oidcAccessToken", token,
                                         "--eval",
                                         "db.runCommand({connectionStatus: 1});");
        return exitCode;
    };

    assert.eq(clientConnect(conn), 0);

    MongoRunner.stopMongod(conn);
})();
