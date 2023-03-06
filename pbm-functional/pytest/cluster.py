import testinfra
import time
import docker
import pymongo
import json
import copy

# the structure of the cluster could be one of
# { _id: "rsname", members: [{host: "host", hidden: boolean, priority: int, arbiterOnly: bool}, ...]} for replicaset
# { mongos: 'host', configserver: { replicaset }, shards: [replicaset1, replicaset2 ...]}


class Cluster:
    def __init__(self, config):
        self.config = config

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        if not isinstance(value, dict):
            raise TypeError("Config must be a dict")

        def validate_rs(rs):
            if not isinstance(rs['_id'], str) or not isinstance(rs['members'], list):
                return False

            if len(rs['members']) == 1 or len(rs['members']) % 2 != 1:
                return False

            arbiter = False
            nodes = []
            for id, member in enumerate(rs['members']):
                if not isinstance(member, dict):
                    return False

                if not set(member.keys()) <= {'host', 'priority', 'arbiterOnly', 'hidden'} and not set(member.keys()) == {'host'}:
                    print(1)
                    return False

                if (not isinstance(member['host'], str) or ('priority' in member and not isinstance(member['priority'], int)) or
                    ('arbiterOnly' in member and not isinstance(member['arbiterOnly'], bool)) or
                        ('hidden' in member and not isinstance(member['hidden'], bool))):
                    print(2)
                    return False

                if id == 0 and set(member.keys()) <= {'priority', 'arbiterOnly', 'hidden'}:
                    print(3)
                    return False

                if member['host'] not in nodes:
                    nodes.append(member['host'])
                else:
                    return False
                if 'arbiterOnly' in member and member['arbiterOnly']:
                    if arbiter:
                        print(4)
                        return False
                    arbiter = True

            return True

        if set(value.keys()) == {'_id', 'members'}:
            if not validate_rs(value):
                raise TypeError("Invalid replicaset config")
        elif set(value.keys()) == {'mongos', 'configserver', 'shards'}:
            if not isinstance(value['configserver'], dict) or not isinstance(value['shards'], list) or not isinstance(value['mongos'], str):
                raise TypeError("Invalid replicaset config")
            nodes = []
            ids = []
            if not validate_rs(value['configserver']):
                raise TypeError(
                    "Invalid configserver defifnition in sharded config")
            else:
                ids.append(value['configserver']['_id'])
                for member in value['configserver']['members']:
                    if member['host'] not in nodes:
                        nodes.append(member['host'])
                    else:
                        raise TypeError(
                            "Duplicated node name in sharded config")
            for shard in value['shards']:
                if not validate_rs(shard):
                    raise TypeError(
                        "Invalid shard definition in sharded config")
                else:
                    if shard['_id'] not in ids:
                        ids.append(shard['_id'])
                    else:
                        raise TypeError(
                            "Duplicated replicaset id in sharded config")
                    for member in shard['members']:
                        if member['host'] not in nodes:
                            nodes.append(member['host'])
                        else:
                            raise TypeError(
                                "Duplicated node name in sharded config")
        else:
            raise TypeError("Invalid config")
        self._config = value

    @property
    def layout(self):
        if set(self.config.keys()) == {'_id', 'members'}:
            return "replicaset"
        else:
            return "sharded"

    @property
    def pbm_cli(self):
        if self.layout == "replicaset":
            return self.config['members'][0]['host']
        else:
            return self.config['configserver']['members'][0]['host']

    @property
    def connection(self):
        if self.layout == "replicaset":
            return "mongodb://root:root@" + self.config['members'][0]['host'] + ":27017/"
        else:
            return "mongodb://root:root@" + self.config['mongos'] + ":27017/"

    @property
    def pbm_hosts(self):
        hosts = []
        if self.layout == "replicaset":
            for host in self.config['members']:
                if "arbiterOnly" in host:
                    if not host['arbiterOnly']:
                        hosts.append(host['host'])
                else:
                    hosts.append(host['host'])
        else:
            for shard in self.config['shards']:
                for host in shard['members']:
                    if "arbiterOnly" in host:
                        if not host['arbiterOnly']:
                            hosts.append(host['host'])
                    else:
                        hosts.append(host['host'])
            for host in self.config['configserver']['members']:
                if "arbiterOnly" in host:
                    if not host['arbiterOnly']:
                        hosts.append(host['host'])
                else:
                    hosts.append(host['host'])
        return hosts

    @property
    def mongod_hosts(self):
        hosts = []
        if self.layout == "replicaset":
            for host in self.config['members']:
                hosts.append(host['host'])
        else:
            for shard in self.config['shards']:
                for host in shard['members']:
                    hosts.append(host['host'])
            for host in self.config['configserver']['members']:
                hosts.append(host['host'])
        return hosts

    @property
    def all_hosts(self):
        hosts = []
        if self.layout == "replicaset":
            for host in self.config['members']:
                hosts.append(host['host'])
        else:
            for shard in self.config['shards']:
                for host in shard['members']:
                    hosts.append(host['host'])
            for host in self.config['configserver']['members']:
                hosts.append(host['host'])
            hosts.append(self.config['mongos'])
        return hosts

    def create(self):
        print("\nCreating cluster:")
        print(self.config)
        if self.layout == "replicaset":
            for host in self.config['members']:
                print("Creating container " + host['host'])
                docker.from_env().containers.run(
                    image='replica_member/local',
                    name=host['host'],
                    hostname=host['host'],
                    detach=True,
                    network='test',
                    environment=["PBM_MONGODB_URI=mongodb://pbm:pbmpass@127.0.0.1:27017",
                                 "MONGODB_EXTRA_ARGS= --port 27017 --replSet " + self.config['_id'] + " --keyFile /etc/keyfile"],
                    volumes=["fs:/backups"]
                )
                if "arbiterOnly" in host:
                    if host['arbiterOnly']:
                        self.__delete_pbm(host['host'])
            time.sleep(5)
            self.__setup_replicaset(self.config)
            self.__setup_authorization(self.config['members'][0]['host'])
        else:
            shards = []
            for shard in self.config['shards']:
                conn = shard['_id'] + "/"
                for host in shard['members']:
                    print("Creating container " + host['host'])
                    docker.from_env().containers.run(
                        image='replica_member/local',
                        name=host['host'],
                        hostname=host['host'],
                        detach=True,
                        network='test',
                        environment=["PBM_MONGODB_URI=mongodb://pbm:pbmpass@127.0.0.1:27017",
                                     "MONGODB_EXTRA_ARGS= --port 27017 --replSet " + shard['_id'] + " --shardsvr --keyFile /etc/keyfile"],
                        volumes=["fs:/backups"]
                    )
                    if 'arbiterOnly' in host:
                        if host['arbiterOnly']:
                            self.__delete_pbm(host['host'])
                    conn = conn + host['host'] + ':27017,'
                conn = conn[:-1]
                shards.append(conn)
            conn = self.config['configserver']['_id'] + "/"
            for host in self.config['configserver']['members']:
                print("Creating container " + host['host'])
                docker.from_env().containers.run(
                    image='replica_member/local',
                    name=host['host'],
                    hostname=host['host'],
                    detach=True,
                    network='test',
                    environment=["PBM_MONGODB_URI=mongodb://pbm:pbmpass@127.0.0.1:27017", "MONGODB_EXTRA_ARGS= --port 27017 --replSet " +
                                 self.config['configserver']['_id'] + " --configsvr --keyFile /etc/keyfile"],
                    volumes=["fs:/backups"]
                )
                if "arbiterOnly" in host:
                    if host['arbiterOnly']:
                        self.__delete_pbm(host['host'])
                conn = conn + host['host'] + ':27017,'
            conn = conn[:-1]
            configdb = conn
            time.sleep(5)
            self.__setup_replicasets(
                self.config['shards'] + [self.config['configserver']])
            self.__setup_authorizations(self.config['shards'])
            print("Creating container " + self.config['mongos'])
            docker.from_env().containers.run(
                image='replica_member/local',
                name=self.config['mongos'],
                hostname=self.config['mongos'],
                command='mongos --keyFile=/etc/keyfile --configdb ' +
                configdb + ' --port 27017 --bind_ip 0.0.0.0',
                detach=True,
                network='test'
            )
            time.sleep(5)
            self.__setup_authorization(self.config['mongos'])
            connection = self.connection
            client = pymongo.MongoClient(connection)
            for shard in shards:
                result = client.admin.command("addShard", shard)
                print("adding shard: " + shard + "\n" + str(result))
        self.restart_pbm_agents()

    def setup_pbm(self):
        node = self.pbm_cli
        n = testinfra.get_host("docker://" + node)
        result = n.check_output('pbm config --file=/etc/pbm.conf --out=json')
        print(json.loads(result))
        time.sleep(5)

    def restart_pbm_agents(self):
        [self.__restart_pbm_agent(node) for node in self.pbm_hosts]

    def make_resync(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        output = n.check_output('pbm config --force-resync')
        print(output)
        timeout = time.time() + 30
        while True:
            logs = self.__find_event_msg("resync", "succeed")
            if logs:
                print(logs)
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)
        time.sleep(5)

    def make_backup(self, type):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        timeout = time.time() + 30
        while True:
            running = self.check_status()
            if not running:
                if type:
                    start = n.check_output(
                        'pbm backup --out=json --type=' + type)
                else:
                    start = n.check_output('pbm backup --out=json')
                name = json.loads(start)['name']
                print("Backup started:")
                print(name)
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)
        timeout = time.time() + 600
        while True:
            running = self.check_status()
            print("current operation:")
            print(running)
            result = self.find_backup(name)
            if result:
                print("Backup found:")
                print(result)
                assert result['status'] == 'done'
                return name
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)

    def make_restore(self, name, **kwargs):
        if self.layout == "sharded":
            client = pymongo.MongoClient(self.connection)
            result = client.admin.command("balancerStop")
            print("Stopping balancer")
            self.stop_mongos()
        n = testinfra.get_host("docker://" + self.pbm_cli)
        timeout = time.time() + 600
        while True:
            if not self.check_status():
                print("Restore started: " + name)
                output = n.check_output('pbm restore ' + name + ' --wait')
                print(output)
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)
        for key, value in kwargs.items():
            if key == "restart_cluster" and value:
                self.restart()
                self.restart_pbm_agents()
        if self.layout == "sharded":
            client = pymongo.MongoClient(self.connection)
            self.start_mongos()
            result = client.admin.command("balancerStart")
        for key, value in kwargs.items():
            if key == "make_resync" and value:
                self.make_resync()
            if key == "check_pbm_status" and value:
                self.check_pbm_status()

    def destroy(self):
        print("\nDestroying cluster:")
        print(self.all_hosts)
        for host in self.all_hosts:
            try:
                container = docker.from_env().containers.get(host)
                container.remove(force=True)
                print("Container {} was removed".format(host))
            except docker.errors.NotFound:
                print("Container {} was not found".format(host))

    def restart(self):
        print("\nRestarting cluster")
        print(self.mongod_hosts)
        for host in self.mongod_hosts:
            print("Restarting " + host)
            docker.from_env().containers.get(host).restart()
        time.sleep(10)

    def stop_mongos(self):
        if self.layout == "sharded":
            print("Stopping " + self.config['mongos'])
            docker.from_env().containers.get(self.config['mongos']).stop()

    def start_mongos(self):
        if self.layout == "sharded":
            print("Starting " + self.config['mongos'])
            docker.from_env().containers.get(self.config['mongos']).start()
            time.sleep(5)

    def enable_pitr(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        n.check_output(
            "pbm config --set pitr.enabled=true --set pitr.compression=none")
        timeout = time.time() + 600
        while True:
            if self.check_pitr():
                break
            if time.time() > timeout:
                assert False
            time.sleep(5)

    def disable_pitr(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        n.check_output("pbm config --set pitr.enabled=false")
        timeout = time.time() + 600
        while True:
            if not self.check_pitr():
                break
            if time.time() > timeout:
                assert False
            time.sleep(5)

    def exec_pbm_cli(self, params):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        return n.run("pbm " + params)

    def __setup_replicaset(self, replicaset):
        primary = replicaset['members'][0]['host']
        primary = testinfra.get_host("docker://" + primary)
        print("\nSetup " + replicaset['_id'])
        rs = copy.deepcopy(replicaset)
        rs['members'][0]['priority'] = 1000
        for id, data in enumerate(rs['members']):
            rs['members'][id]['_id'] = id
            rs['members'][id]['host'] = rs['members'][id]['host'] + ":27017"
        init_rs = ('\'config =' +
                   json.dumps(rs) +
                   ';\nrs.initiate(config);\'')
        print(init_rs)
        logs = primary.check_output("mongo --quiet --eval " + init_rs)
        print(logs)

    def __setup_replicasets(self, replicasets):
        [self.__setup_replicaset(rs) for rs in replicasets]
        time.sleep(5)

    def __setup_authorization(self, node):
        primary = testinfra.get_host("docker://" + node)
        self.__wait_for_primary(node, "mongodb://127.0.0.1:27017")
        print("\nSetup authorization on " + node)
        print("\nAdding root user")
        init_root_user = '\'db.getSiblingDB("admin").createUser({ user: "root", pwd: "root", roles: [ "root", "userAdminAnyDatabase", "clusterAdmin" ] });\''
        logs = primary.check_output("mongo --quiet --eval " + init_root_user)
        print(logs)
        print("\nAdding pbm role")
        init_pbm_role = '\'db.getSiblingDB("admin").createRole({"role": "pbmAnyAction","privileges":[{"resource":{"anyResource":true},"actions":["anyAction"]}],"roles":[]});\''
        logs = primary.check_output(
            "mongo -u root -p root --quiet --eval " + init_pbm_role)
        print(logs)
        print("\nAdding pbm user")
        init_pbm_user = ('\'db.getSiblingDB("admin").createUser({user:"pbm",pwd:"pbmpass","roles":[' +
                         '{"db":"admin","role":"readWrite","collection":""},' +
                         '{"db":"admin","role":"backup" },' +
                         '{"db":"admin","role":"clusterMonitor" },' +
                         '{"db":"admin","role":"restore" },' +
                         '{"db":"admin","role":"pbmAnyAction" }]});\'')
        logs = primary.check_output(
            "mongo -u root -p root --quiet --eval " + init_pbm_user)
        print(logs)

    def __setup_authorizations(self, replicasets):
        [self.__setup_authorization(rs['members'][0]['host'])
         for rs in replicasets]
        time.sleep(5)

    def __check_primary(self, node, connection):
        primary = testinfra.get_host("docker://" + node)
        result = primary.check_output(
            "mongo " + connection + " --quiet --eval 'db.isMaster().ismaster'")
        if result.lower() == 'true':
            return True
        else:
            return False

    def __wait_for_primary(self, node, connection):
        timeout = time.time() + 600
        while True:
            if self.__check_primary(node, connection):
                break
            else:
                print("Waiting for " + node + " to become primary")
            if time.time() > timeout:
                assert False
            time.sleep(5)

    def __delete_pbm(self, node):
        n = testinfra.get_host("docker://" + node)
        n.check_output("supervisorctl stop pbm-agent")
        n.check_output("rm -rf /etc/supervisord.d/pbm-agent.ini")

    def __find_event_msg(self, event, msg):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        command = "pbm logs --tail=0 --out=json --event=" + event
        logs = n.check_output(command)
        for log in json.loads(logs):
            if log['msg'] == msg:
                return log
                break

    def find_backup(self, name):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        list = n.check_output('pbm list --out=json')
        parsed_list = json.loads(list)
        if parsed_list['snapshots']:
            for snapshot in parsed_list['snapshots']:
                if snapshot['name'] == name:
                    return snapshot
                    break

    def get_pbm_logs(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        logs = n.check_output("pbm logs -s D --tail=0")
        print(logs)

    def check_status(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        status = n.check_output('pbm status --out=json')
        running = json.loads(status)['running']
        if running:
            return running

    def check_pitr(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        status = n.check_output('pbm status --out=json')
        running = json.loads(status)['pitr']['run']
        return bool(running)

    def check_pbm_status(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output('pbm status --out=json')
        print("\nPBM status:")
        parsed_result = json.loads(result)
        print(json.dumps(parsed_result, indent=4))
        nodes = []
        for replicaset in parsed_result['cluster']:
            for host in replicaset['nodes']:
                if host['role'] != "A":
                    nodes.append(host)
                    assert host['ok'] == True
        assert len(nodes) == len(self.pbm_hosts)

    def __check_pbm_service(self, node):
        n = testinfra.get_host("docker://" + node)
        service = n.service("pbm-agent")
        assert service.is_running

    def __restart_pbm_agent(self, node):
        print("Restarting pbm-agent on node " + node)
        n = testinfra.get_host("docker://" + node)
        n.check_output('supervisorctl restart pbm-agent')
