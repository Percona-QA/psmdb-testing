xdb = db.getSiblingDB('$external')
xdb.createUser({user: "arn:aws:iam::119175775298:role/jenkins-psmdb-slave",roles: [{role: "userAdminAnyDatabase", db: "admin"}]})
