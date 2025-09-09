(function() {
  'use strict';

  print("MARKER: using test_ldap_simple.js on host=" + hostname());

  // Bring up a clean server to create the local admin+role.
  var conn = MongoRunner.runMongod();
  var db = conn.getDB("admin");

  db.createUser({ user: 'admin', pwd: 'password', roles: ['root'] });
  db.createRole({
    role: "cn=testwriters,ou=groups,dc=percona,dc=com",
    privileges: [],
    roles: ["userAdminAnyDatabase", "clusterMonitor", "clusterManager", "clusterAdmin"]
  });
  db.logout();
  MongoRunner.stopMongod(conn);

  // Now start with LDAP.
  let opts = {
    restart: conn,
    auth: '',
    ldapServers: '127.0.0.1:389',
    ldapTransportSecurity: 'none',
    ldapBindMethod: 'simple',
    ldapQueryUser: 'cn=admin,dc=percona,dc=com',
    ldapQueryPassword: 'secret',
    ldapAuthzQueryTemplate: 'ou=groups,dc=percona,dc=com??sub?(member={PROVIDED_USER})',
    setParameter: { authenticationMechanisms: 'PLAIN,SCRAM-SHA-256,SCRAM-SHA-1' },
    noCleanData: true
  };

  // DEBUG: show exactly what MongoRunner will pass to mongod
  print("DEBUG argv =", tojson(MongoRunner.arrOptions("mongod", opts)));

  conn = MongoRunner.runMongod(opts);
  assert(conn, "Cannot start mongod instance");

  const username = 'cn=exttestrw,ou=people,dc=percona,dc=com';
  const password = 'exttestrw9a5S';

  var clientConnect = function(conn) {
    const exitCode = runMongoProgram("/usr/bin/mongo",
      "--port", conn.port,
      "--authenticationDatabase", "$external",
      "--authenticationMechanism", "PLAIN",
      "--username", username,
      "--password", password,
      "--verbose",
      "--eval", "db.runCommand({connectionStatus:1});");
    return exitCode;
  };

  assert.eq(clientConnect(conn), 0);
  MongoRunner.stopMongod(conn);
})();