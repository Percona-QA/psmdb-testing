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

    var dbPath = MongoRunner.dataPath

    MongoRunner.stopMongod(conn);

    // test command line parameters related to LDAP authorization
    var conn = MongoRunner.runMongod({
        auth: '',
        ldapServers: '127.0.0.1:389',
        ldapTransportSecurity: 'none',
        ldapBindMethod: 'simple',
        ldapQueryUser: 'dc=percona,dc=com',
        ldapQueryPassword: 'secret',
        ldapAuthzQueryTemplate: 'ou=groups,dc=percona,dc=com??sub?(member={PROVIDED_USER})',
        setParameter: {authenticationMechanisms: 'PLAIN,SCRAM-SHA-256,SCRAM-SHA-1'},
        dbpath: dbPath,
        noCleanData: true
    });

    assert(conn, "Cannot start mongod instance");

    var db = conn.getDB('$external');
    const username = 'cn=exttestrw,ou=people,dc=percona,dc=com';
    const password = 'exttestrw9a5S'

    print('authenticating ' + username);
    assert(db.auth({
        user: username,
        pwd: password,
        mechanism: 'PLAIN'
    }));

    checkConnectionStatus(username, db.runCommand({connectionStatus: 1}));

    db.logout();

    MongoRunner.stopMongod(conn);
})();
