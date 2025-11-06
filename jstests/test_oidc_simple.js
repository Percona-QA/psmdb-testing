(function() {
    'use strict';

    var conn = MongoRunner.runMongod();
    var db = conn.getDB("external");

    // create administrator
    db.createRole({
        role: "keycloak/Everyone", privileges: [], roles: [ "readWriteAnyDatabase"]
    });

    db.createUser({
        user: 'keycloak/pbmclient',
        pwd: 'password',
        roles: [ 'keycloak/Everyone' ]
    });

    db.logout();

    MongoRunner.stopMongod(conn);

    // test command line parameters related to LDAP authorization
    conn = MongoRunner.runMongod({
        restart: conn,
        auth: '',
        setParameter: {authenticationMechanisms: 'SCRAM-SHA-1,MONGODB-OIDC', oidcIdentityProviders: '[{' +
                '     "issuer": "https://keycloak:8443/realms/test",' +
                '     "clientId": "test-client",' +
                '     "audience": "account",' +
                '     "authNamePrefix": "keycloak",' +
                '     "useAuthorizationClaim": false,' +
                '     "supportsHumanFlows": false,' +
                '     "principalName": "client_id"' +
                '  }]'},
        noCleanData: true
    });

    assert(conn, "Cannot start mongod instance");

    const username = 'cn=exttestrw,ou=people,dc=percona,dc=com';
    const password = 'exttestrw9a5S'

    var clientConnect = function(conn) {
        const exitCode = runMongoProgram("/usr/bin/mongo",
                                         "--port",
                                         conn.port,
                                         "--authenticationDatabase",
                                         '$external',
                                         "--authenticationMechanism",
                                         "PLAIN",
                                         "--username",
                                         username,
                                         "--password",
                                         password,
                                         "--verbose",
                                         "--eval",
                                         "db.runCommand({connectionStatus: 1});");
        return exitCode;
    };

    assert.eq(clientConnect(conn), 0);

    MongoRunner.stopMongod(conn);
})();
