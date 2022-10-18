(function() {
    'use strict';

    // prepare for the auth mode
    var conn = MongoRunner.runMongod();
    var db = conn.getDB("admin");

    // create administrator
    db.createUser({
        user: 'admin',
        pwd: 'password',
        roles: [ 'root' ]
    });

    var ext = conn.getDB("$external");

    ext.createUser({
        user: 'exttestrw@PERCONATEST.COM',
        roles: [ {role: "userAdminAnyDatabase", db: "admin"} ]
    });

    var dbPath = MongoRunner.dataPath

    MongoRunner.stopMongod(conn);


    var conn = MongoRunner.runMongod({
        auth: '',
        setParameter: {authenticationMechanisms: 'GSSAPI'},
        env: 'KRB5_KTNAME=/etc/mongodb.keytab',
        dbpath: dbPath,
        noCleanData: true
    });

    assert(conn, "Cannot start mongod instance");

    function _runCmd(cmd) {
        runProgram('bash', '-c', cmd);
    }

    _runCmd("kadmin.local -q 'addprinc -pw exttestrw exttestrw'");
    _runCmd("kinit exttestrw <<<'exttestrw'");

    var db = conn.getDB('$external');
    const username = 'exttestrw@PERCONATEST.COM';

    print('authenticating ' + username);
    assert(db.auth({
        user: username,
        mechanism: 'GSSAPI'
    }));

    checkConnectionStatus(username, db.runCommand({connectionStatus: 1}));

    db.logout();

    MongoRunner.stopMongod(conn);
})();
