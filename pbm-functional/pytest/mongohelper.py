import testinfra
import time
import docker
import pymongo
import concurrent.futures

def check_primary(node,connection):
    primary = testinfra.get_host("docker://" + node)
    result=primary.check_output("mongo " + connection + " --quiet --eval 'db.isMaster().ismaster'")
    if result.lower() == 'true':
        return True
    else:
        return False

def wait_for_primary(node,connection):
    timeout = time.time() + 600
    while True:
        if check_primary(node,connection):
            break
        else:
            print("waiting for " + node + " to become primary")
        if time.time() > timeout:
            assert False
        time.sleep(5)

def prepare_rs(replicaset):
    rsname = list(replicaset.keys())[0]
    primary = replicaset[rsname][0]
    primary = testinfra.get_host("docker://" + primary)
    print("\nsetup " + rsname)
    init_rs = ( '\'config = {"_id":"' +
        rsname +
        '","members":['
        )
    for id, node in enumerate(replicaset[rsname]):
        if id == 0:
            priority = 2
        else:
            priority = 1
        init_rs = ( init_rs +
            '{"_id":' + str(id) +
            ',"host":"' + node +
            ':27017","priority":' + str(priority) +
            '},')
    init_rs = init_rs[:-1] + ']};\nrs.initiate(config);\''
    print(init_rs)
    logs = primary.check_output("mongo --quiet --eval " + init_rs)
    print(logs)
    time.sleep(5)

def setup_authorization(node):
    primary = testinfra.get_host("docker://" + node)
    wait_for_primary(node,"mongodb://127.0.0.1:27017")
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
            executor.submit(prepare_rs,rs)

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

def wait_for_primary_parallel(cluster,connection):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for rs in cluster:
            rsname = list(rs.keys())[0]
            primary = rs[rsname][0]
            executor.submit(wait_for_primary,primary,connection)

def create_sharded(mongos,cluster):
    shards=[]
    rsauth=[]
    for rs in cluster:
        rsname = list(rs.keys())[0]
        conn = rsname + "/"
        if "cfg" in rsname:
            svr = " --configsvr"
        else:
            svr = " --shardsvr"
        for node in rs[rsname]:
            docker.from_env().containers.run(
                image='replica_member/local',
                name=node,
                hostname=node,
                detach=True,
                network='test',
                environment=["PBM_MONGODB_URI=mongodb://pbm:pbmpass@127.0.0.1:27017","MONGODB_EXTRA_ARGS= --port 27017 --replSet " + rsname + svr + " --keyFile /etc/keyfile"],
                volumes=["fs:/backups"]
                )
            conn = conn + node + ':27017,'
        conn = conn[:-1]
        print("\nSetup rs: " + conn)
        if "cfg" in rsname:
            configdb = conn
        else:
            shards.append(conn)
            rsauth.append(rs)
    time.sleep(5)
    prepare_rs_parallel(cluster)
    setup_authorization_parallel(rsauth)
    docker.from_env().containers.run(
        image='replica_member/local',
        name=mongos,
        hostname=mongos,
        command='mongos --keyFile=/etc/keyfile --configdb ' + configdb + ' --port 27017 --bind_ip 0.0.0.0',
        detach=True,
        network='test'
        )
    time.sleep(5)
    setup_authorization(mongos)
    connection = "mongodb://root:root@" + mongos + ":27017"
    client = pymongo.MongoClient(connection)
    for shard in shards:
        result=client.admin.command("addShard", shard)
        print("adding shard: " + shard + "\n" + str(result))

    return connection

def destroy_sharded(mongos,cluster):
    print("\nCleanup")
    nodes=[mongos]
    for rs in cluster:
        rsname = list(rs.keys())[0]
        for node in rs[rsname]:
            nodes.append(node)
    for node in nodes:
        if docker.from_env().containers.list(all=True, filters={'name': node}):
            print("Destroying container: " + node )
            docker.from_env().containers.get(node).remove(force=True)

def create_replicaset(rs):
    rsname=list(rs.keys())[0]
    for node in rs[rsname]:
        docker.from_env().containers.run(
            image='replica_member/local',
            name=node,
            hostname=node,
            detach=True,
            network='test',
            environment=["PBM_MONGODB_URI=mongodb://pbm:pbmpass@127.0.0.1:27017","MONGODB_EXTRA_ARGS= --port 27017 --replSet " + rsname + " --keyFile /etc/keyfile"],
            volumes=["fs:/backups"]
            )
    time.sleep(5)
    prepare_rs(rs)
    setup_authorization_parallel([rs])

def destroy_replicaset(replicaset):
    rsname = list(replicaset.keys())[0]
    for node in rseplicaset[rsname]:
         if docker.from_env().containers.list(all=True, filters={'name': node}):
             docker.from_env().containers.get(node).remove(force=True)
