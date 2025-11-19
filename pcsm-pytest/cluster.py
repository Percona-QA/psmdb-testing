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

    @property
    def is_sharded(self):
        return self.layout == "sharded"

    # returns mongodb connection string to cluster
    @property
    def connection(self):
        if self.layout == "replicaset":
            hosts = ",".join(f"{member['host']}:27017" for member in self.config["members"])
            return (f"mongodb://root:root@{hosts}/"f"?replicaSet={self.config['_id']}")
        else:
            return "mongodb://root:root@" + self.config['mongos'] + ":27017/"

    @property
    def csync_connection(self):
        if self.layout == "replicaset":
            hosts = ",".join(f"{member['host']}:27017" for member in self.config["members"])
            return (f"mongodb://root:root@{hosts}/"f"?replicaSet={self.config['_id']}")
        else:
            return "mongodb://csync:test1234@" + self.config['mongos'] + ":27017/"

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
        # account for 2 clusters running in parallel
        calculated_limit = int((mem_per_container // 2))
        # set minimum memory limit for systems with low resources
        min_mem_limit = int(3 * 1024 * 1024 * 1024)
        return max(calculated_limit, min_mem_limit)

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

    # restart primary node in each RS
    def restart_primary(self, delay=0, force=False):
        client = pymongo.MongoClient(self.connection)
        container_names = []
        if self.layout == "replicaset":
            ismaster = client.admin.command("isMaster")
            primary_host = ismaster["primary"]
            for member in self.config["members"]:
                if primary_host.startswith(member["host"]):
                    container_names.append(member["host"])
                    break
            else:
                raise Exception("Primary node is not found in RS")
        elif self.layout == "sharded":
            configserver_hosts = self.config["configserver"]["members"]
            configserver_conn = "mongodb://root:root@" + configserver_hosts[0]["host"] + ":27017/?replicaSet=" + self.config["configserver"]["_id"]
            cfg_client = pymongo.MongoClient(configserver_conn)
            cfg_ismaster = cfg_client.admin.command("isMaster")
            cfg_primary = cfg_ismaster["primary"]
            for member in configserver_hosts:
                if cfg_primary.startswith(member["host"]):
                    container_names.append(member["host"])
                    break
            else:
                raise Exception("Primary node is not found in config RS")
            for shard in self.config["shards"]:
                shard_hosts = shard["members"]
                shard_conn = "mongodb://root:root@" + shard_hosts[0]["host"] + ":27017/?replicaSet=" + shard["_id"]
                shard_client = pymongo.MongoClient(shard_conn)
                shard_ismaster = shard_client.admin.command("isMaster")
                shard_primary = shard_ismaster["primary"]
                for member in shard_hosts:
                    if shard_primary.startswith(member["host"]):
                        container_names.append(member["host"])
                        break
                else:
                    raise Exception(f"Primary node is not found in {shard['_id']} RS")
        for container_name in container_names:
            container = docker.from_env().containers.get(container_name)
            if force:
                container.kill()
                Cluster.log("Killed " + container_name)
            else:
                container.stop()
                Cluster.log("Stopped " + container_name)
            if delay > 0:
                time.sleep(delay)
            container.start()
            Cluster.log("Started " + container_name)

    # step down primary node in each RS
    def stepdown_primary(self, stepdown_timeout=10):
        def step_down(client, name, timeout):
            try:
                client.admin.command("replSetStepDown", timeout)
            except pymongo.errors.AutoReconnect:
                Cluster.log(f"{name} primary stepped down successfully")
                return True
            except pymongo.errors.OperationFailure as e:
                Cluster.log(f"{name} stepdown failed: {e}. Retrying with force...")
                try:
                    client.admin.command("replSetStepDown", timeout, force=True)
                    Cluster.log(f"{name} primary stepped down with force")
                    return True
                except (pymongo.errors.AutoReconnect, pymongo.errors.OperationFailure) as e2:
                    Cluster.log(f"{name} stepdown with force failed: {e2}")
                    return False
            return True
        def is_primary(client):
            try:
                hello = client.admin.command("hello")
                return hello.get("isWritablePrimary", False)
            except Exception as e:
                Cluster.log(f"Error checking primary status: {e}")
                return False
        def try_stepdown(uri, name):
            try:
                with pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
                    if is_primary(client):
                        if step_down(client, name, stepdown_timeout):
                            Cluster.log(f"Primary in {name} is stepped down")
            except Exception as e:
                Cluster.log(f"Error handling client for {name}: {e}")
        if self.layout == "replicaset":
            try_stepdown(self.connection, "Replica set")
        elif self.layout == "sharded":
            config = self.config["configserver"]
            config_host = next((m["host"] for m in config["members"]), None)
            config_rs = config["_id"]
            config_uri = f"mongodb://root:root@{config_host}:27017/?replicaSet={config_rs}"
            try_stepdown(config_uri, "Config server")
            for shard in self.config["shards"]:
                shard_host = next((m["host"] for m in shard["members"]), None)
                shard_rs = shard["_id"]
                shard_uri = f"mongodb://root:root@{shard_host}:27017/?replicaSet={shard_rs}"
                try_stepdown(shard_uri, f"Shard {shard_rs}")

    # disconnects all containers from the network and reconnects them after delay
    def network_interruption(self, delay=5):
        docker_client = docker.from_env()
        network = docker_client.networks.get("test")
        all_hosts = []
        if self.layout == "replicaset":
            all_hosts = [m["host"] for m in self.config["members"]]
        elif self.layout == "sharded":
            all_hosts += [m["host"] for m in self.config["configserver"]["members"]]
            for shard in self.config["shards"]:
                all_hosts += [m["host"] for m in shard["members"]]
        containers = [docker_client.containers.get(name) for name in all_hosts]
        Cluster.log(f"Disconnecting {len(containers)} containers from the network...")
        for c in containers:
            try:
                network.disconnect(c, force=True)
                Cluster.log(f"Disconnected {c.name}")
            except Exception as e:
                Cluster.log(f"Failed to disconnect {c.name}: {e}")
        time.sleep(delay)
        Cluster.log("Reconnecting containers to the network...")
        for c in containers:
            try:
                network.connect(c)
                Cluster.log(f"Reconnected {c.name}")
            except Exception as e:
                Cluster.log(f"Failed to reconnect {c.name}: {e}")

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
        Cluster.log(f"Setting up replicaset {replicaset.get('_id', 'unknown')}")
        primary = replicaset['members'][0]['host']
        primary = testinfra.get_host("docker://" + primary)
        rs = copy.deepcopy(replicaset)
        for id, data in enumerate(rs['members']):
            rs['members'][id]['_id'] = id
            rs['members'][id]['host'] = rs['members'][id]['host'] + ":27017"
        rs['settings'] = {'electionTimeoutMillis': 2000}
        init_rs = ('\'config =' +
                   json.dumps(rs) +
                   ';rs.initiate(config);\'')
        max_iterations = 10
        wait_time = 0.5
        for i in range(max_iterations):
            result = primary.run("mongosh --quiet --eval " + init_rs)
            if result.rc == 0 or (result.rc != 0 and 'already initialized' in result.stderr.lower()):
                break
            time.sleep(wait_time)
        Cluster.log("Setup replicaset " + json.dumps(rs) + ":\n" + result.stdout.strip())

    def __setup_replicasets(self, replicasets):
        Cluster.log(f"Setting up {len(replicasets)} replicasets: {[rs.get('_id', 'unknown') for rs in replicasets]}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(replicasets)) as executor:
            futures = []
            for rs in replicasets:
                future = executor.submit(Cluster.setup_replicaset, rs)
                futures.append(future)
            for future in futures:
                future.result()

    @staticmethod
    def setup_authorization(host):
        primary = testinfra.get_host("docker://" + host)
        Cluster.wait_for_primary(host, "mongodb://127.0.0.1:27017")
        Cluster.log("Setup authorization on " + host)
        Cluster.log("Adding root user on " + host)
        init_root_user = '\'db.getSiblingDB("admin").createUser({ user: "root", pwd: "root", roles: [ "root", "userAdminAnyDatabase", "clusterAdmin" ] });\''
        primary.check_output("mongosh --quiet --eval " + init_root_user)
        Cluster.log("Adding system user on " + host)
        init_s_user = '\'db.getSiblingDB("admin").createUser({ user: "system", pwd: "system", roles: [ "__system" ] });\''
        primary.check_output("mongosh -u root -p root --quiet --eval " + init_s_user)
        Cluster.log("Adding csync user on " + host)
        csync_user = ('\'db.getSiblingDB("admin").createUser({user:"csync",pwd:"test1234","roles":[' +
                         '{"db":"admin","role":"backup" },' +
                         '{"db":"admin","role":"clusterMonitor" },' +
                         '{"db":"admin","role":"clusterManager" },' +
                         '{"db":"admin","role":"restore" },' +
                         '{"db":"admin","role":"readWriteAnyDatabase" }]});\'')
        x509_csync_user = (
            '\'db.getSiblingDB("$external").runCommand({createUser:"emailAddress=pcsm@percona.com,CN=pcsm,OU=client,O=Percona,L=SanFrancisco,ST=California,C=US","roles":['
            + '{"db":"admin","role":"backup" },'
            + '{"db":"admin","role":"clusterMonitor" },'
            + '{"db":"admin","role":"clusterManager" },'
            + '{"db":"admin","role":"restore" },'
            + '{"db":"admin","role":"readWriteAnyDatabase" }]});\'')
        primary.check_output(
            "mongosh -u root -p root --quiet --eval " + csync_user)
        primary.check_output(
            "mongosh -u root -p root --quiet --eval " + x509_csync_user)

    def __setup_authorizations(self, replicasets):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(replicasets)) as executor:
            futures = [executor.submit(Cluster.setup_authorization, rs['members'][0]['host'])
                       for rs in replicasets]
            for future in futures:
                future.result()

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
            time.sleep(0.5)

    def wait_for_primaries(self):
        Cluster.log(self.primary_hosts)
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.primary_hosts)) as executor:
            futures = []
            for primary in self.primary_hosts:
                future = executor.submit(Cluster.wait_for_primary, primary,
                                "mongodb://root:root@127.0.0.1:27017")
                futures.append(future)
            for future in futures:
                future.result()

    @staticmethod
    def log(*args, **kwargs):
        print("[%s]" % (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S'),*args, **kwargs)
