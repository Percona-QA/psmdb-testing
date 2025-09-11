(function() {
    'use strict';

    var conn = MongoRunner.runMongod();
    var db = conn.getDB("admin");

    // create administrator
    db.createUser({
        user: 'admin',
        pwd: 'password',
        roles: [ 'root' ]
    });

    db.createRole({
        role: "cn=testwriters,ou=groups,dc=percona,dc=com", privileges: [], roles: [ "userAdminAnyDatabase", "clusterMonitor", "clusterManager", "clusterAdmin"]
    });

    db.logout();

    MongoRunner.stopMongod(conn);

    // test command line parameters related to LDAP authorization
    conn = MongoRunner.runMongod({
        restart: conn,
        auth: '',
        ldapServers: '127.0.0.1:389',
        ldapTransportSecurity: 'none',
        ldapBindMethod: 'simple',
        ldapQueryUser: 'cn=admin,dc=percona,dc=com',
        ldapQueryPassword: 'secret',
        ldapAuthzQueryTemplate: 'ou=groups,dc=percona,dc=com??sub?(member={PROVIDED_USER})',
        setParameter: {authenticationMechanisms: 'PLAIN,SCRAM-SHA-256,SCRAM-SHA-1'},
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
