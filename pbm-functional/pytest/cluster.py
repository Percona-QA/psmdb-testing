import testinfra
import time
import docker
import pymongo
import json
import copy
import concurrent.futures

# the structure of the cluster could be one of
# 1. { _id: "rsname", members: [{host: "host", hidden: boolean, priority: int, arbiterOnly: bool}, ...]} for replicaset
# https://www.mongodb.com/docs/manual/reference/replica-configuration/#std-label-replica-set-configuration-document
# required parameters are: _id (str), members (array of members), host (str)
# allowed parameters are: hidden (bool), priority (int), arbiterOnly( bool )
# the first member of replica is not allowed to be hidden or arbiter - it's reserved to become primary member for each replica with priority 1000
# 2. { mongos: 'host', configserver: { replicaset }, shards: [replicaset1, replicaset2 ...]} for sharded
# required parameters: are mongos (str), configserver (replicaset), shards (array of replicasets)
# mongos - hostname for mongos instance


class Cluster:
    def __init__(self, config, **kwargs):
        self.config = config
        self.mongod_extra_args = kwargs.get('mongod_extra_args', "")
        self.mongod_datadir = kwargs.get('mongod_datadir', "/var/lib/mongo")
        self.pbm_mongodb_uri = kwargs.get('pbm_mongodb_uri', "mongodb://pbm:pbmpass@127.0.0.1:27017/?authSource=admin")

    @property
    def config(self):
        return self._config

    @property
    def mongod_extra_args(self):
        return self._mongod_extra_args

    @mongod_extra_args.setter
    def mongod_extra_args(self, value):
        assert isinstance(value, str)
        self._mongod_extra_args = value

    @property
    def mongod_datadir(self):
        return self._mongod_datadir

    @mongod_datadir.setter
    def mongod_datadir(self, value):
        assert isinstance(value, str)
        self._mongod_datadir = value

    @property
    def pbm_mongodb_uri(self):
        return self._pbm_mongodb_uri

    @pbm_mongodb_uri.setter
    def pbm_mongodb_uri(self, value):
        assert isinstance(value, str)
        self._pbm_mongodb_uri = value

    # config validator
    @config.setter
    def config(self, value):
        assert isinstance(value, dict)

        def validate_rs(rs):
            assert isinstance(rs['_id'], str) and isinstance(
                rs['members'], list)
            assert len(rs['members']) % 2 == 1
            arbiter = False
            hosts = []
            for id, member in enumerate(rs['members']):
                assert isinstance(member, dict)
                assert set(member.keys()) <= {
                    'host', 'priority', 'arbiterOnly', 'hidden'}
                assert 'host' in member and isinstance(member['host'], str)
                if 'priority' in member:
                    assert isinstance(member['priority'], int)
                if 'arbiterOnly' in member:
                    assert isinstance(member['arbiterOnly'], bool)
                if 'hidden' in member:
                    assert isinstance(member['hidden'], bool)
                if id == 0:
                    assert set(member.keys()) == {'host'}
                if member['host'] not in hosts:
                    hosts.append(member['host'])
                else:
                    assert False
                if 'arbiterOnly' in member and member['arbiterOnly']:
                    if arbiter:
                        assert False
                    arbiter = True
            return True
        if set(value.keys()) == {'_id', 'members'}:
            assert validate_rs(value)
        elif set(value.keys()) == {'mongos', 'configserver', 'shards'}:
            assert isinstance(value['configserver'], dict) and isinstance(
                value['shards'], list) and isinstance(value['mongos'], str)
            hosts = []
            ids = []
            assert validate_rs(value['configserver'])
            ids.append(value['configserver']['_id'])
            for member in value['configserver']['members']:
                if member['host'] not in hosts:
                    hosts.append(member['host'])
                else:
                    assert False
            for shard in value['shards']:
                assert validate_rs(shard)
                if shard['_id'] not in ids:
                    ids.append(shard['_id'])
                else:
                    assert False
                for member in shard['members']:
                    if member['host'] not in hosts:
                        hosts.append(member['host'])
                    else:
                        assert False
        else:
            assert False
        self._config = value

    # returns replicaset or sharded
    @property
    def layout(self):
        if set(self.config.keys()) == {'_id', 'members'}:
            return "replicaset"
        else:
            return "sharded"

    # returns primary host of configserver or primary member in case of repicaset layout
    @property
    def pbm_cli(self):
        if self.layout == "replicaset":
            return self.config['members'][0]['host']
        else:
            return self.config['configserver']['members'][0]['host']

    # returns mongodb connection string to cluster, for replicaset layout we excpect that the first member will always be primary
    @property
    def connection(self):
        if self.layout == "replicaset":
            return "mongodb://root:root@" + self.config['members'][0]['host'] + ":27017/"
        else:
            return "mongodb://root:root@" + self.config['mongos'] + ":27017/"

    # returns array of hosts with pbm-agent - all hosts except mongos and arbiters
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

    # returns array of hosts with arbiter - it's necessary for physical/incremental restores
    @property
    def arbiter_hosts(self):
        hosts = []
        if self.layout == "replicaset":
            for host in self.config['members']:
                if "arbiterOnly" in host:
                    if host['arbiterOnly']:
                        hosts.append(host['host'])
        else:
            for shard in self.config['shards']:
                for host in shard['members']:
                    if "arbiterOnly" in host:
                        if host['arbiterOnly']:
                            hosts.append(host['host'])
            for host in self.config['configserver']['members']:
                if "arbiterOnly" in host:
                    if host['arbiterOnly']:
                        hosts.append(host['host'])
        return hosts

    # returns array of hosts with mongod - all hosts except mongos
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

    # returns primary members for all shards and primary for configserver
    @property
    def primary_hosts(self):
        hosts = []
        if self.layout == "replicaset":
            hosts.append(self.config['members'][0]['host'])
        else:
            for shard in self.config['shards']:
                hosts.append(shard['members'][0]['host'])
            hosts.append(self.config['configserver']['members'][0]['host'])
        return hosts

    # returns all hosts of the cluster
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

    # configures and starts all docker-containers, creates necessary layout, setups athorization
    def create(self):
        print("\nCreating cluster:")
        print(self.config)
        if self.layout == "replicaset":
            for host in self.config['members']:
                print("Creating container " + host['host'])
                pbm_mongodb_uri = copy.deepcopy(self.pbm_mongodb_uri)
                if "authMechanism=GSSAPI" in pbm_mongodb_uri:
                    pbm_mongodb_uri = pbm_mongodb_uri.replace("127.0.0.1",host['host'])
                docker.from_env().containers.run(
                    image='replica_member/local',
                    name=host['host'],
                    hostname=host['host'],
                    detach=True,
                    network='test',
                    environment=["PBM_MONGODB_URI=" + pbm_mongodb_uri, "DATADIR=" + self.mongod_datadir, "KRB5_KTNAME=/keytabs/" + host['host'] + "/mongodb.keytab",
                                 "MONGODB_EXTRA_ARGS= --port 27017 --replSet " + self.config['_id'] + " --keyFile /etc/keyfile " + self.mongod_extra_args],
                    volumes=["fs:/backups","keytabs:/keytabs"]
                )
                if "arbiterOnly" in host:
                    if host['arbiterOnly']:
                        self.__delete_pbm(host['host'])
            time.sleep(5)
            Cluster.setup_replicaset(self.config)
            Cluster.setup_authorization(self.config['members'][0]['host'],self.pbm_mongodb_uri)
        else:
            shards = []
            for shard in self.config['shards']:
                conn = shard['_id'] + "/"
                for host in shard['members']:
                    print("Creating container " + host['host'])
                    pbm_mongodb_uri = copy.deepcopy(self.pbm_mongodb_uri)
                    if "authMechanism=GSSAPI" in pbm_mongodb_uri:
                        pbm_mongodb_uri = pbm_mongodb_uri.replace("127.0.0.1",host['host'])
                    docker.from_env().containers.run(
                        image='replica_member/local',
                        name=host['host'],
                        hostname=host['host'],
                        detach=True,
                        network='test',
                        environment=["PBM_MONGODB_URI=" + pbm_mongodb_uri, "DATADIR=" + self.mongod_datadir, "KRB5_KTNAME=/keytabs/" + host['host'] + "/mongodb.keytab",
                                     "MONGODB_EXTRA_ARGS= --port 27017 --replSet " + shard['_id'] + " --shardsvr --keyFile /etc/keyfile " + self.mongod_extra_args, "KRB5_TRACE=/dev/stderr"],
                        volumes=["fs:/backups","keytabs:/keytabs"]
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
                pbm_mongodb_uri = copy.deepcopy(self.pbm_mongodb_uri)
                if "authMechanism=GSSAPI" in pbm_mongodb_uri:
                    pbm_mongodb_uri = pbm_mongodb_uri.replace("127.0.0.1",host['host'])
                docker.from_env().containers.run(
                    image='replica_member/local',
                    name=host['host'],
                    hostname=host['host'],
                    detach=True,
                    network='test',
                    environment=["PBM_MONGODB_URI=" + pbm_mongodb_uri, "DATADIR=" + self.mongod_datadir, "KRB5_KTNAME=/keytabs/" + host['host'] + "/mongodb.keytab",
                                 "MONGODB_EXTRA_ARGS= --port 27017 --replSet " +
                                 self.config['configserver']['_id'] + " --configsvr --keyFile /etc/keyfile " + self.mongod_extra_args],
                    volumes=["fs:/backups","keytabs:/keytabs"]
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
            print("\nCreating container " + self.config['mongos'])
            docker.from_env().containers.run(
                image='replica_member/local',
                name=self.config['mongos'],
                hostname=self.config['mongos'],
                command='mongos --keyFile=/etc/keyfile --configdb ' +
                configdb + ' --port 27017 --bind_ip 0.0.0.0',
                detach=True,
                network='test'
            )
            time.sleep(1)
            Cluster.setup_authorization(self.config['mongos'],self.pbm_mongodb_uri)
            connection = self.connection
            client = pymongo.MongoClient(connection)
            for shard in shards:
                result = client.admin.command("addShard", shard)
                print("adding shard: " + shard + "\n" + str(result))
        self.restart_pbm_agents()

    # setups pbm from default config-file, minio as storage
    def setup_pbm(self):
        host = self.pbm_cli
        n = testinfra.get_host("docker://" + host)
        result = n.check_output('pbm config --file=/etc/pbm.conf --out=json')
        print(json.loads(result))
        time.sleep(5)

    # pbm --force-resync
    def make_resync(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output('pbm config --force-resync --out json')
        parsed_result = json.loads(result)
        print(parsed_result)
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

    # creates backup based on type (no checking input - it's hack for situation like 'incremental --base')
    def make_backup(self, type):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        timeout = time.time() + 30
        while True:
            if not self.get_status()['running']:
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
            status = self.get_status()
            print("current operation:")
            print(status['running'])
            if status['backups']['snapshot']:
                for snapshot in status['backups']['snapshot']:
                    if snapshot['name'] == name:
                        if snapshot['status'] == 'done':
                            print("Backup found:")
                            print(snapshot)
                            return name
                            break
                        elif snapshot['status'] == 'error':
                            assert False, snapshot['error']
                            break
            if time.time() > timeout:
                assert False, "Backup timeout exceeded"
            time.sleep(1)

    # restores backup from name, accept extra-args:
    # 1. restart_cluster = bool - restarts cluster after the restore, necessary for physical/incremental
    # 2. make_resync = bool - `pbm --force-resync` after the restore
    # 3. check_pbm_status = bool - check `pbm status` output, raises error if any agent is failed
    def make_restore(self, name, **kwargs):
        if self.layout == "sharded":
            client = pymongo.MongoClient(self.connection)
            result = client.admin.command("balancerStop")
            client.close()
            print("Stopping balancer")
            print(result)
            self.stop_mongos()
        self.stop_arbiters()
        n = testinfra.get_host("docker://" + self.pbm_cli)
        timeout = time.time() + 600
        while True:
            if not self.get_status()['running']:
                print("Restore started: " + name)
                output = n.check_output('pbm restore ' + name + ' --wait')
                print(output)
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)
        self.start_arbiters()
        for key, value in kwargs.items():
            if key == "restart_cluster" and value:
                self.restart()
                self.restart_pbm_agents()
            if key == "make_resync" and value:
                self.make_resync()
            if key == "check_pbm_status" and value:
                self.check_pbm_status()
        if self.layout == "sharded":
            self.start_mongos()
            client = pymongo.MongoClient(self.connection)
            result = client.admin.command("balancerStart")
            print("Starting balancer")
            print(result)

    # destroys cluster
    def destroy(self):
        print("\nDestroying cluster:")
        print(self.all_hosts)
        # the last resort to catch possible failures if timeout exceeded
        for host in self.mongod_hosts:
            try:
                container = docker.from_env().containers.get(host)
                result = container.exec_run(
                    'cat /var/lib/mongo/pbm.restore.log', stderr=False)
                if result.exit_code == 0:
                    print(
                        "\n!!!!Possible failure on {}, file pbm.restore.log was found:".format(host))
                    print(result.output.decode('utf-8'))
            except docker.errors.APIError:
                pass
        for host in self.all_hosts:
            try:
                container = docker.from_env().containers.get(host)
                container.remove(force=True)
                print("Container {} was removed".format(host))
            except docker.errors.NotFound:
                pass

    # restarts all containers with mongod sequentially
    def restart(self):
        for host in self.mongod_hosts:
            print("Restarting " + host)
            docker.from_env().containers.get(host).restart()
        time.sleep(1)
        self.wait_for_primaries()

    # stops mongos container
    def stop_mongos(self):
        if self.layout == "sharded":
            print("Stopping " + self.config['mongos'])
            docker.from_env().containers.get(self.config['mongos']).kill()

    # starts mongos container
    def start_mongos(self):
        if self.layout == "sharded":
            print("Starting " + self.config['mongos'])
            docker.from_env().containers.get(self.config['mongos']).start()
            time.sleep(1)
            Cluster.wait_for_primary(
                self.config['mongos'], "mongodb://root:root@127.0.0.1:27017")

    # stops mongod's on arbiter hosts
    def stop_arbiters(self):
        for host in self.arbiter_hosts:
            print("Stopping arbiter on " + host)
            n = testinfra.get_host("docker://" + host)
            n.check_output("supervisorctl stop mongod")

    # starts mongod's on arbiter hosts
    def start_arbiters(self):
        for host in self.arbiter_hosts:
            print("Starting arbiter on " + host)
            n = testinfra.get_host("docker://" + host)
            n.check_output("supervisorctl start mongod")

    # enables PITR
    def enable_pitr(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output(
            "pbm config --set pitr.enabled=true --set pitr.compression=none --out json")
        print("Enabling PITR:")
        print(result)
        timeout = time.time() + 600
        while True:
            if self.check_pitr():
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)

    # disables PITR
    def disable_pitr(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output(
            "pbm config --set pitr.enabled=false --out json")
        print("Disabling PITR:")
        print(result)
        timeout = time.time() + 600
        while True:
            if not self.check_pitr():
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)

    # executes any pbm command e.g. cluster.exec_pbm_cli("status"), doesn't raise any errors, output from
    # https://testinfra.readthedocs.io/en/latest/modules.html#testinfra.host.Host.run
    def exec_pbm_cli(self, params):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        return n.run("pbm " + params)

    @staticmethod
    def setup_replicaset(replicaset):
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
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for rs in replicasets:
                executor.submit(Cluster.setup_replicaset, rs)

    @staticmethod
    def setup_authorization(host,uri):
        primary = testinfra.get_host("docker://" + host)
        Cluster.wait_for_primary(host, "mongodb://127.0.0.1:27017")
        print("\nSetup authorization on " + host)
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
        x509_pbm_user = ('\'db.getSiblingDB("$external").runCommand({createUser:"emailAddress=pbm@percona.com,CN=pbm,OU=client,O=Percona,L=SanFrancisco,ST=California,C=US","roles":[' +
                         '{"db":"admin","role":"readWrite","collection":""},' +
                         '{"db":"admin","role":"backup" },' +
                         '{"db":"admin","role":"clusterMonitor" },' +
                         '{"db":"admin","role":"restore" },' +
                         '{"db":"admin","role":"pbmAnyAction" }]});\'')
        krb_pbm_user = ('\'db.getSiblingDB("$external").runCommand({createUser:"pbm@PERCONATEST.COM","roles":[' +
                         '{"db":"admin","role":"readWrite","collection":""},' +
                         '{"db":"admin","role":"backup" },' +
                         '{"db":"admin","role":"clusterMonitor" },' +
                         '{"db":"admin","role":"restore" },' +
                         '{"db":"admin","role":"pbmAnyAction" }]});\'')
        logs = primary.check_output(
            "mongo -u root -p root --quiet --eval " + init_pbm_user)
        print(logs)
        if "authMechanism=MONGODB-X509" in uri:
            logs = primary.check_output(
                "mongo -u root -p root --quiet --eval " + x509_pbm_user)
            print(logs)
        if "authMechanism=GSSAPI" in uri:
            logs = primary.check_output(
                "mongo -u root -p root --quiet --eval " + krb_pbm_user)
            print(logs)

    def __setup_authorizations(self, replicasets):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for rs in replicasets:
                executor.submit(Cluster.setup_authorization,
                                rs['members'][0]['host'], self.pbm_mongodb_uri)
        time.sleep(1)

    @staticmethod
    def wait_for_primary(host, connection):
        n = testinfra.get_host("docker://" + host)
        timeout = time.time() + 60
        print("Checking ismaster() on host: " + host)
        while True:
            result = n.run(
                "mongo " + connection + " --quiet --eval 'db.hello().isWritablePrimary'")
            if 'true' in result.stdout.lower():
                print("Host " + host + " became primary")
                return True
                break
            elif 'mongoservererror' in result.stderr.lower():
                assert False, result.stderr
            else:
                print("Waiting for " + host + " to became primary")
            if time.time() > timeout:
                assert False
            time.sleep(3)
        print("\n")

    def wait_for_primaries(self):
        print(self.primary_hosts)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for primary in self.primary_hosts:
                executor.submit(Cluster.wait_for_primary, primary,
                                "mongodb://root:root@127.0.0.1:27017")

    def __delete_pbm(self, host):
        n = testinfra.get_host("docker://" + host)
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

    def get_status(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        status = n.check_output('pbm status --out=json')
        return json.loads(status)

    def get_version(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        status = n.check_output('pbm version --out=json')
        return json.loads(status)

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
        hosts = []
        for replicaset in parsed_result['cluster']:
            for host in replicaset['nodes']:
                if host['role'] != "A":
                    hosts.append(host)
                    assert host['ok'] == True
        assert len(hosts) == len(self.pbm_hosts)

    @staticmethod
    def restart_pbm_agent(host):
        print("Restarting pbm-agent on host " + host)
        n = testinfra.get_host("docker://" + host)
        n.check_output('supervisorctl restart pbm-agent')
        assert n.supervisor('pbm-agent').is_running

    def restart_pbm_agents(self):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for host in self.pbm_hosts:
                executor.submit(Cluster.restart_pbm_agent, host)
        time.sleep(5)

    @staticmethod
    def downgrade_single(host,**kwargs):
        tarball=kwargs.get('tarball',"")
        n = testinfra.get_host("docker://" + host)
        n.check_output('supervisorctl stop pbm-agent')
        if tarball:
            n.check_output('curl -Lf -o /tmp/pbm.tar.gz ' + tarball)
            n.check_output("tar -xf /tmp/pbm.tar.gz --transform 's,^/*[^/]*,,S' -C /usr/bin")
        else:
            n.check_output('cp -rf /pbm-old/* /usr/bin/')
        n.check_output('supervisorctl start pbm-agent')
        assert n.supervisor('pbm-agent').is_running

    def downgrade(self,**kwargs):
        print("\nDowngrading PBM")
        ver = self.get_version()
        print("Current PBM version:")
        print(ver)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for host in self.pbm_hosts:
                executor.submit(Cluster.downgrade_single, host, **kwargs)
        time.sleep(5)
        ver = self.get_version()
        print("New PBM version:")
        print(ver)

    @staticmethod
    def upgrade_single(host):
        n = testinfra.get_host("docker://" + host)
        n.check_output('supervisorctl stop pbm-agent')
        n.check_output('cp -rf /pbm-new/* /usr/bin/')
        n.check_output('supervisorctl start pbm-agent')
        assert n.supervisor('pbm-agent').is_running

    def upgrade(self):
        print("\nUpgrading PBM")
        ver = self.get_version()
        print("Current PBM version:")
        print(ver)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for host in self.pbm_hosts:
                executor.submit(Cluster.upgrade_single, host)
        time.sleep(5)
        ver = self.get_version()
        print("New PBM version:")
        print(ver)

    def get_logs(self):
        for container in self.all_hosts:
            header = "Logs from {name}:".format(name=container)
            print(header, '\n', "=" * len(header))
            try:
                print(docker.from_env().containers.get(
                    container).logs().decode("utf-8", errors="replace"))
            except docker.errors.NotFound:
                print()
