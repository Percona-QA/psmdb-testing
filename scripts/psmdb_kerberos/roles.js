xdb = db.getSiblingDB('$external')
xdb.createUser({user: "exttestro@PERCONATEST.COM",roles: [{role: "read", db: "admin"}]})
xdb.createUser({user: "exttestrw@PERCONATEST.COM",roles: [{role: "userAdminAnyDatabase", db: "admin"}]})
