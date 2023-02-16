import testinfra
import time

def prepare_rs(rsname,nodes,sharding):
    for node in nodes:
        n = testinfra.get_host("docker://" + node)
        if sharding == "shardsrv":
            n.check_output('sed -E "s/^#replication:/sharding:\\n  clusterRole: shardsvr\\nreplication:\\n  replSetName: ' + rsname + '/" -i /etc/mongod.conf')
        elif sharding == "configsvr":
            n.check_output('sed -E "s/^#replication:/sharding:\\n  clusterRole: configsvr\\nreplication:\\n  replSetName: ' + rsname + '/" -i /etc/mongod.conf')
        else:
            n.check_output('sed -E "s/^#replication:/replication:\\n  replSetName: ' + rsname + '/" -i /etc/mongod.conf')
        n.check_output('sed -E "s/^  bindIp: 127.0.0.1/  bindIp: 0.0.0.0/" -i /etc/mongod.conf')
        n.check_output('systemctl restart mongod')
    time.sleep(5)
    primary = testinfra.get_host("docker://" + nodes[0])
    print("\nsetup " + rsname)
    init_rs = ( '\'config = {"_id":"' + 
        rsname + 
        '","members":[{"_id":0,"host":"' +
        nodes[0] +
        ':27017","priority":2},{"_id":1,"host":"' +
        nodes[1] +
        ':27017","priority":1},{"_id":2,"host":"' +
        nodes[2] +
        ':27017","priority":1}]};\nrs.initiate(config);\'' )
    print(init_rs)
    logs = primary.check_output("mongo --quiet --eval " + init_rs)
    print(logs)
    time.sleep(60)
    print("\nsetup authorization")
    print("\nadding root user")
    init_root_user = '\'db.getSiblingDB("admin").createUser({ user: "root", pwd: "root", roles: [ "root", "userAdminAnyDatabase", "clusterAdmin" ] });\''
    logs = primary.check_output("mongo --quiet --eval " + init_root_user)
    print(logs)
    print("\nadding pbm role")
    init_pbm_role = '\'db.getSiblingDB("admin").createRole({"role": "pbmAnyAction","privileges":[{"resource":{"anyResource":true},"actions":["anyAction"]}],"roles":[]});\''
    logs = primary.check_output("mongo -u root -p root --quiet --eval " + init_pbm_role)
    print(logs)
    print("\nadding pbm user")
    init_pbm_user = ('\'db.getSiblingDB("admin").createUser({user:"pbm",pwd:"pbmpass","roles":[' +
        '{"db":"admin","role":"readWrite","collection":""},' +
        '{"db":"admin","role":"backup" },' +
        '{"db":"admin","role":"clusterMonitor" },' +
        '{"db":"admin","role":"restore" },' +
        '{"db":"admin","role":"pbmAnyAction" }]});\'' )
    logs = primary.check_output("mongo -u root -p root --quiet --eval " + init_pbm_user)
    print(logs)    

def restart_mongod(nodes):
    for node in nodes:
        n = testinfra.get_host("docker://" + node)
        n.check_output('systemctl restart mongod')
