import testinfra
import time
import docker
import pymongo
import json
import copy
import concurrent.futures
import psutil
from datetime import datetime

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
        self.mongod_datadir = kwargs.get('mongod_datadir', "/data/db")
        self.mongo_image = kwargs.get('mongo_image', "mongodb/local")

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
                    'host', 'priority', 'arbiterOnly', 'hidden', 'secondaryDelaySecs', 'slaveDelay', 'votes', 'buildIndexes'}
                assert 'host' in member and isinstance(member['host'], str)
                if id == 0:
                    assert set(member.keys()) == {'host'}
                if 'priority' in member:
                    assert isinstance(member['priority'], int)
                if 'arbiterOnly' in member:
                    assert isinstance(member['arbiterOnly'], bool)
                if 'hidden' in member:
                    assert isinstance(member['hidden'], bool)
                if 'secondaryDelaySecs' in member:
                    assert isinstance(member['secondaryDelaySecs'], int)
                if 'slaveDelay' in member:
                    assert isinstance(member['slaveDelay'], int)
                if 'votes' in member:
                    assert isinstance(member['votes'], int)
                    assert member['votes'] in [0,1]
                    if member['votes'] == 0:
                        assert member['priority'] == 0
                if 'buildIndexes' in member:
                    assert isinstance(member['buildIndexes'], bool)
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

    # returns mongodb connection string to cluster, for replicaset layout we except that the first member will always be primary
    @property
    def connection(self):
        if self.layout == "replicaset":
            return "mongodb://root:root@" + self.config['members'][0]['host'] + ":27017/?replicaSet=" + self.config['_id']
        else:
            return "mongodb://root:root@" + self.config['mongos'] + ":27017/"

    @property
    def mlink_connection(self):
        if self.layout == "replicaset":
            return "mongodb://mlink:test1234@" + self.config['members'][0]['host'] + ":27017/?replicaSet=" + self.config['_id']
        else:
            return "mongodb://mlink:test1234@" + self.config['mongos'] + ":27017/"

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

    @property
    def entrypoint(self):
        if self.layout == "replicaset":
            return self.config['members'][0]['host']
        else:
            return self.config['mongos']

    @staticmethod
    def calculate_mem_limits(config, layout):
        total_mem = psutil.virtual_memory().total
        mem_pool = int(total_mem * 0.5)
        if layout == "replicaset":
            num_containers = len(config['members'])
        else:
            num_containers = len(config['configserver']['members'])
            num_containers += sum(len(shard['members']) for shard in config['shards'])
            num_containers += 1  # for mongos
        mem_per_container = mem_pool // num_containers
        return mem_per_container // 2 # account for 2 clusters running in parallel during tests

    # configures and starts all docker-containers, creates necessary layout, setups athorization
    def create(self):
        start = time.time()
        Cluster.log("Creating cluster: " + str(self.config))

        mem_limit = self.calculate_mem_limits(self.config, self.layout)
        Cluster.log(f"Memory limit per container: {mem_limit // (1024 ** 2)} MB")

        if self.layout == "replicaset":
            for host in self.config['members']:
                Cluster.log("Creating container " + host['host'])
                cmd = f"mongod --port 27017 --bind_ip 0.0.0.0 --dbpath {self.mongod_datadir} --replSet {self.config['_id']} --keyFile /etc/keyfile {self.mongod_extra_args}"
                docker.from_env().containers.run(
                    image=self.mongo_image,
                    name=host['host'],
                    hostname=host['host'],
                    detach=True,
                    network='test',
                    mem_limit=mem_limit,
                    memswap_limit=mem_limit,
                    command=cmd
                )
            time.sleep(5)
            Cluster.setup_replicaset(self.config)
            Cluster.setup_authorization(self.config['members'][0]['host'])
        else:
            shards = []
            for shard in self.config['shards']:
                conn = shard['_id'] + "/"
                for host in shard['members']:
                    Cluster.log("Creating container " + host['host'])
                    cmd = f"mongod --port 27017 --bind_ip 0.0.0.0 --dbpath {self.mongod_datadir} --replSet {shard['_id']} --shardsvr --keyFile /etc/keyfile {self.mongod_extra_args}"
                    docker.from_env().containers.run(
                        image=self.mongo_image,
                        name=host['host'],
                        hostname=host['host'],
                        detach=True,
                        network='test',
                        mem_limit=mem_limit,
                        memswap_limit=mem_limit,
                        command=cmd
                    )
                    conn = conn + host['host'] + ':27017,'
                conn = conn[:-1]
                shards.append(conn)
            conn = self.config['configserver']['_id'] + "/"
            for host in self.config['configserver']['members']:
                Cluster.log("Creating container " + host['host'])
                cmd = f"mongod --port 27017 --bind_ip 0.0.0.0 --dbpath {self.mongod_datadir} --replSet {self.config['configserver']['_id']} --configsvr --keyFile /etc/keyfile {self.mongod_extra_args}"
                docker.from_env().containers.run(
                    image=self.mongo_image,
                    name=host['host'],
                    hostname=host['host'],
                    detach=True,
                    network='test',
                    mem_limit=mem_limit,
                    memswap_limit=mem_limit,
                    command=cmd
                )
                conn = conn + host['host'] + ':27017,'
            conn = conn[:-1]
            configdb = conn
            time.sleep(5)
            self.__setup_replicasets(
                self.config['shards'] + [self.config['configserver']])
            self.__setup_authorizations(self.config['shards'])
            Cluster.log("Creating container " + self.config['mongos'])
            docker.from_env().containers.run(
                image=self.mongo_image,
                name=self.config['mongos'],
                hostname=self.config['mongos'],
                command='mongos --keyFile=/etc/keyfile --configdb ' + configdb + ' --port 27017 --bind_ip 0.0.0.0',
                detach=True,
                network='test',
                mem_limit=mem_limit,
                memswap_limit=mem_limit
            )
            time.sleep(1)
            Cluster.setup_authorization(self.config['mongos'])
            connection = self.connection
            client = pymongo.MongoClient(connection)
            for shard in shards:
                result = client.admin.command("addShard", shard)
                Cluster.log("Adding shard \"" + shard + "\":\n" + str(result))
        duration = time.time() - start
        Cluster.log("The cluster was prepared in {} seconds".format(duration))

    # destroys cluster
    def destroy(self,**kwargs):
        print("\n")
        for host in self.all_hosts:
            try:
                container = docker.from_env().containers.get(host)
                container.remove(v=True,force=True)
                Cluster.log("Container {} was removed".format(host))
            except docker.errors.NotFound:
                pass

    # restarts all containers with mongod sequentially
    def restart(self):
        for host in self.mongod_hosts:
            Cluster.log("Restarting " + host)
            docker.from_env().containers.get(host).restart()
        time.sleep(1)

    # stops mongos container
    def stop_mongos(self):
        if self.layout == "sharded":
            Cluster.log("Stopping " + self.config['mongos'])
            docker.from_env().containers.get(self.config['mongos']).kill()

    # starts mongos container
    def start_mongos(self):
        if self.layout == "sharded":
            Cluster.log("Starting " + self.config['mongos'])
            docker.from_env().containers.get(self.config['mongos']).start()
            time.sleep(1)
            Cluster.wait_for_primary(
                self.config['mongos'], "mongodb://root:root@127.0.0.1:27017")

    @staticmethod
    def setup_replicaset(replicaset):
        primary = replicaset['members'][0]['host']
        primary = testinfra.get_host("docker://" + primary)
        rs = copy.deepcopy(replicaset)
        rs['members'][0]['priority'] = 1000
        for id, data in enumerate(rs['members']):
            rs['members'][id]['_id'] = id
            rs['members'][id]['host'] = rs['members'][id]['host'] + ":27017"
        init_rs = ('\'config =' +
                   json.dumps(rs) +
                   ';rs.initiate(config);\'')
        result = primary.check_output("mongosh --quiet --eval " + init_rs)
        Cluster.log("Setup replicaset " + json.dumps(rs) + ":\n" + result)

    def __setup_replicasets(self, replicasets):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for rs in replicasets:
                executor.submit(Cluster.setup_replicaset, rs)

    @staticmethod
    def setup_authorization(host):
        primary = testinfra.get_host("docker://" + host)
        Cluster.wait_for_primary(host, "mongodb://127.0.0.1:27017")
        Cluster.log("Setup authorization on " + host)
        Cluster.log("Adding root user on " + host)
        init_root_user = '\'db.getSiblingDB("admin").createUser({ user: "root", pwd: "root", roles: [ "root", "userAdminAnyDatabase", "clusterAdmin" ] });\''
        logs = primary.check_output("mongosh --quiet --eval " + init_root_user)
        Cluster.log("Adding system user on " + host)
        init_s_user = '\'db.getSiblingDB("admin").createUser({ user: "system", pwd: "system", roles: [ "__system" ] });\''
        logs = primary.check_output("mongosh -u root -p root --quiet --eval " + init_s_user)
        Cluster.log("Adding mlink user on " + host)
        mlink_user = ('\'db.getSiblingDB("admin").createUser({user:"mlink",pwd:"test1234","roles":[' +
                         '{"db":"admin","role":"backup" },' +
                         '{"db":"admin","role":"clusterMonitor" },' +
                         '{"db":"admin","role":"clusterManager" },' +
                         '{"db":"admin","role":"restore" },' +
                         '{"db":"admin","role":"readWriteAnyDatabase" }]});\'')
        logs = primary.check_output(
            "mongosh -u root -p root --quiet --eval " + mlink_user)

    def __setup_authorizations(self, replicasets):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for rs in replicasets:
                executor.submit(Cluster.setup_authorization,
                                rs['members'][0]['host'])
        time.sleep(1)

    @staticmethod
    def wait_for_primary(host, connection):
        n = testinfra.get_host("docker://" + host)
        timeout = time.time() + 60
        Cluster.log("Checking ismaster() on host " + host)
        while True:
            result = n.run(
                "mongosh " + connection + " --quiet --eval 'db.hello().isWritablePrimary'")
            if 'true' in result.stdout.lower():
                Cluster.log("Host " + host + " became primary")
                return True
                break
            elif 'mongoservererror' in result.stderr.lower():
                assert False, result.stderr
            else:
                Cluster.log("Waiting for " + host + " to became primary")
            if time.time() > timeout:
                assert False
            time.sleep(3)

    def wait_for_primaries(self):
        Cluster.log(self.primary_hosts)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for primary in self.primary_hosts:
                executor.submit(Cluster.wait_for_primary, primary,
                                "mongodb://root:root@127.0.0.1:27017")

    @staticmethod
    def log(*args, **kwargs):
        print("[%s]" % (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S'),*args, **kwargs)
