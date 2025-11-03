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
    db.logout();

    var ext = conn.getDB("$external");

    //create user
    ext.createUser({
        user: 'exttestrw@PERCONATEST.COM',
        roles: [ {role: "userAdminAnyDatabase", db: "admin"} ]
    });

    MongoRunner.stopMongod(conn);

    // start mongod with GSSAPI authentication enabled
    conn = MongoRunner.runMongod({
        restart: conn,
        auth: '',
        setParameter: {authenticationMechanisms: 'GSSAPI'},
        env: {KRB5_KTNAME: '/etc/mongodb.keytab'},
        noCleanData: true,
    });

    assert(conn, "Cannot start mongod instance");

    function _runCmd(cmd) {
        runProgram('bash', '-c', cmd);
    }

    //add principal
    _runCmd('docker exec kerberos sh -lc "kadmin.local -q \'addprinc -pw exttestrw exttestrw@PERCONATEST.COM\'"');
    _runCmd("kinit exttestrw <<<'exttestrw'");

    //check connection
    var clientConnect = function(conn) {
        const exitCode = runMongoProgram("/usr/bin/mongo",
                                         "--host",
                                         getHostName(),
                                         "--port",
                                         conn.port,
                                         "--authenticationDatabase",
                                         '$external',
                                         "--authenticationMechanism",
                                         "GSSAPI",
                                         "--username",
                                         'exttestrw@PERCONATEST.COM',
                                         "--verbose",
                                         "--eval",
                                         "db.runCommand({connectionStatus: 1});");
        return exitCode;
    };

    assert.eq(clientConnect(conn), 0);

    MongoRunner.stopMongod(conn);
})();
