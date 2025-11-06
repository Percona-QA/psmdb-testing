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

    // db.createUser({
    //     user: 'keycloak/pbmclient',
    //     roles: [ 'keycloak/Everyone' ]
    // });

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

    // test command line parameters related to LDAP authorization
    conn = MongoRunner.runMongod({
        restart: conn,
        auth: '',
        setParameter: {authenticationMechanisms: 'SCRAM-SHA-1,MONGODB-OIDC', oidcIdentityProviders: oidcProviders},
        noCleanData: true
    });

    assert(conn, "Cannot start mongod instance");

    var clientConnect = function(conn) {
        const exitCode = runMongoProgram("/usr/bin/mongo",
                                         "--port",
                                         conn.port,
                                         "--authenticationDatabase",
                                         '$external',
                                         "--authenticationMechanism",
                                         "MONGODB-OIDC",
                                         "--verbose",
                                         "--eval",
                                         "db.runCommand({connectionStatus: 1});");
        return exitCode;
    };

    assert.eq(clientConnect(conn), 0);

    MongoRunner.stopMongod(conn);
})();
