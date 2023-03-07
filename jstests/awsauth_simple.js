(function() {
    'use strict';

    var conn = MongoRunner.runMongod({
        auth: '',
        setParameter: {authenticationMechanisms: 'MONGODB-AWS'}
    });

    assert(conn, "Cannot start mongod instance");

    var ext = conn.getDB("$external");
    var iam = _getEnv('IAM')
    ext.createUser({
        user: iam,
        roles: [ {role: "userAdminAnyDatabase", db: "admin"} ]
    });
    ext.logout();

    assert(ext.auth({mechanism: 'MONGODB-AWS'}));
    ext.logout();

    MongoRunner.stopMongod(conn);
})();
