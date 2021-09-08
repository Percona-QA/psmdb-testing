db.createRole({role: "cn=testwriters,ou=groups,dc=percona,dc=com", privileges: [], roles: [ "userAdminAnyDatabase", "clusterMonitor", "clusterManager", "clusterAdmin"]})
db.createRole({role: "cn=testreaders,ou=groups,dc=percona,dc=com", privileges: [], roles: [ "read"]})
