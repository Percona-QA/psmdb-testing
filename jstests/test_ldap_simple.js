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
        ldapQueryUser: 'dc=percona,dc=com',
        ldapQueryPassword: 'secret',
        ldapAuthzQueryTemplate: 'ou=groups,dc=percona,dc=com??sub?(member={PROVIDED_USER})',
        setParameter: {authenticationMechanisms: 'PLAIN,SCRAM-SHA-256,SCRAM-SHA-1'},
        noCleanData: true
    });

    assert(conn, "Cannot start mongod instance");

    var ext = conn.getDB('$external');
    const username = 'cn=exttestrw,ou=people,dc=percona,dc=com';
    const password = 'exttestrw9a5S'

    print('authenticating ' + username);
    assert(ext.auth({
        user: username,
        pwd: password,
        mechanism: 'PLAIN'
    }));

    assert(ext.runCommand({
        connectionStatus: 1
    }));

    ext.logout();

    MongoRunner.stopMongod(conn);
})();
