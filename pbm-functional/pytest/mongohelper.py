import testinfra
import time
import concurrent.futures

def check_primary(node):
    primary = testinfra.get_host("docker://" + node)
    result=primary.check_output("mongo --quiet --eval 'db.isMaster().ismaster'")
    if result.lower() == 'true':
        return True
    else:
        return False

def wait_for_primary(node):
    timeout = time.time() + 600
    while True:
        if check_primary(node):
            break
        else:
            print("waiting for " + node + " to become primary")
        if time.time() > timeout:
            assert False
        time.sleep(5)

def prepare_rs(rsname,nodes):
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
    time.sleep(5)

def setup_authorization(node):
    primary = testinfra.get_host("docker://" + node)
    wait_for_primary(node)
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
    time.sleep(5)

def prepare_rs_parallel(replicasets):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for rs in replicasets:
            rsname = list(rs.keys())[0]
            nodes = rs[rsname]
            executor.submit(prepare_rs,rsname,nodes)

def setup_authorization_parallel(replicasets):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for rs in replicasets:
            rsname = list(rs.keys())[0]
            primary = rs[rsname][0]
            executor.submit(setup_authorization,primary)

def restart_mongod(nodes):
    for node in nodes:
        print("restarting mongodb on node " + node)
        n = testinfra.get_host("docker://" + node)
        n.check_output('supervisorctl restart mongod')
        time.sleep(1)

def wait_for_primary_parallel(replicasets):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for rs in replicasets:
            rsname = list(rs.keys())[0]
            primary = rs[rsname][0]
            executor.submit(wait_for_primary,primary)
