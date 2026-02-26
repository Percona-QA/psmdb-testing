import testinfra
import time
import docker
import pymongo
import json
import copy
import concurrent.futures
import os
import re
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
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
        self.mongod_extra_args = kwargs.get('mongod_extra_args', " --setParameter=logicalSessionRefreshMillis=10000 --setParameter=shutdownTimeoutMillisForSignaledShutdown=300")
        self.mongod_datadir = kwargs.get('mongod_datadir', "/var/lib/mongo")
        self.pbm_mongodb_uri = kwargs.get('pbm_mongodb_uri', "mongodb://pbm:pbmpass@127.0.0.1:27017/?authSource=admin")
        self.cmd_stdout = ""
        self.cmd_stderr = ""
        self.extra_environment = kwargs.get('extra_environment') or {}

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
            #assert len(rs['members']) % 2 == 1
            arbiter = False
            hosts = []
            for id, member in enumerate(rs['members']):
                assert isinstance(member, dict)
                assert set(member.keys()) <= {
                    'host', 'priority', 'arbiterOnly', 'hidden', 'secondaryDelaySecs', 'slaveDelay', 'votes', 'buildIndexes', 'tags', 'mongod_extra_args'}
                assert 'host' in member and isinstance(member['host'], str)
                if id == 0:
                    allowed_first_keys = {'host', 'tags', 'mongod_extra_args'}
                    assert set(member.keys()).issubset(allowed_first_keys)
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
                if 'tags' in member:
                    assert isinstance(member['tags'], dict)
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
        start = time.time()
        Cluster.log("Creating cluster: " + str(self.config))
        if self.layout == "replicaset":
            for host in self.config['members']:
                Cluster.log("Creating container " + host['host'])
                pbm_mongodb_uri = copy.deepcopy(self.pbm_mongodb_uri)
                if 'tags' in host and 'ce' in host['tags'] and host['tags']['ce'] == "true":
                    autostart_ce = "true"
                    autostart_psmdb = "false"
                else:
                    autostart_ce = "false"
                    autostart_psmdb = "true"
                if "authMechanism=GSSAPI" in pbm_mongodb_uri:
                    pbm_mongodb_uri = pbm_mongodb_uri.replace("127.0.0.1",host['host'])
                mongod_args = host.pop("mongod_extra_args", self.mongod_extra_args)
                env_list = ["AUTOSTART_CE=" + autostart_ce, "AUTOSTART_PSMDB=" + autostart_psmdb,
                            "PBM_MONGODB_URI=" + pbm_mongodb_uri, "DATADIR=" + self.mongod_datadir,
                            "KRB5_KTNAME=/keytabs/" + host['host'] + "/mongodb.keytab",
                            "KRB5_CLIENT_KTNAME=/keytabs/" + host['host'] + "/pbm.keytab",
                            "MONGODB_EXTRA_ARGS= --port 27017 --replSet " + self.config['_id'] + " --keyFile /etc/keyfile " + mongod_args,
                            "GOCOVERDIR=/gocoverdir/reports"]
                env_list += [k + "=" + str(v) for k, v in self.extra_environment.items()]
                docker.from_env().containers.run(
                    image='replica_member/local',
                    name=host['host'],
                    hostname=host['host'],
                    detach=True,
                    network='test',
                    environment=env_list,
                    volumes=["fs:/backups","keytabs:/keytabs","gocoverdir:/gocoverdir"],
                    cap_add=["NET_ADMIN", "NET_RAW"]
                )
                if "arbiterOnly" in host:
                    if host['arbiterOnly']:
                        self.__delete_pbm(host['host'])
            time.sleep(2)
            Cluster.setup_replicaset(self.config)
            Cluster.setup_authorization(self.config['members'][0]['host'],self.pbm_mongodb_uri)
        else:
            shards = []
            for shard in self.config['shards']:
                conn = shard['_id'] + "/"
                for host in shard['members']:
                    Cluster.log("Creating container " + host['host'])
                    pbm_mongodb_uri = copy.deepcopy(self.pbm_mongodb_uri)
                    if 'tags' in host and 'ce' in host['tags'] and host['tags']['ce'] == 'true':
                        autostart_ce = "true"
                        autostart_psmdb = "false"
                    else:
                        autostart_ce = "false"
                        autostart_psmdb = "true"
                    if "authMechanism=GSSAPI" in pbm_mongodb_uri:
                        pbm_mongodb_uri = pbm_mongodb_uri.replace("127.0.0.1",host['host'])
                    mongod_args = host.pop("mongod_extra_args", self.mongod_extra_args)
                    env_list = ["AUTOSTART_CE=" + autostart_ce, "AUTOSTART_PSMDB=" + autostart_psmdb,
                                "PBM_MONGODB_URI=" + pbm_mongodb_uri, "DATADIR=" + self.mongod_datadir,
                                "KRB5_KTNAME=/keytabs/" + host['host'] + "/mongodb.keytab",
                                "KRB5_CLIENT_KTNAME=/keytabs/" + host['host'] + "/pbm.keytab",
                                "KRB5_TRACE=/dev/stderr",
                                "MONGODB_EXTRA_ARGS= --port 27017 --replSet " + shard['_id'] + " --shardsvr --keyFile /etc/keyfile " + mongod_args,
                                "GOCOVERDIR=/gocoverdir/reports"]
                    env_list += [k + "=" + str(v) for k, v in self.extra_environment.items()]
                    docker.from_env().containers.run(
                        image='replica_member/local',
                        name=host['host'],
                        hostname=host['host'],
                        detach=True,
                        network='test',
                        environment=env_list,
                        volumes=["fs:/backups","keytabs:/keytabs","gocoverdir:/gocoverdir"]
                    )
                    if 'arbiterOnly' in host:
                        if host['arbiterOnly']:
                            self.__delete_pbm(host['host'])
                    if 'hidden' not in host or not host['hidden']:
                        conn = conn + host['host'] + ':27017,'
                conn = conn[:-1]
                shards.append(conn)
            conn = self.config['configserver']['_id'] + "/"
            for host in self.config['configserver']['members']:
                Cluster.log("Creating container " + host['host'])
                pbm_mongodb_uri = copy.deepcopy(self.pbm_mongodb_uri)
                if 'tags' in host and 'ce' in host['tags'] and host['tags']['ce'] == 'true':
                    autostart_ce = "true"
                    autostart_psmdb = "false"
                else:
                    autostart_ce = "false"
                    autostart_psmdb = "true"
                if "authMechanism=GSSAPI" in pbm_mongodb_uri:
                    pbm_mongodb_uri = pbm_mongodb_uri.replace("127.0.0.1",host['host'])
                mongod_args = host.pop("mongod_extra_args", self.mongod_extra_args)
                env_list = ["AUTOSTART_CE=" + autostart_ce, "AUTOSTART_PSMDB=" + autostart_psmdb,
                            "PBM_MONGODB_URI=" + pbm_mongodb_uri, "DATADIR=" + self.mongod_datadir,
                            "KRB5_KTNAME=/keytabs/" + host['host'] + "/mongodb.keytab",
                            "KRB5_CLIENT_KTNAME=/keytabs/" + host['host'] + "/pbm.keytab",
                            "KRB5_TRACE=/dev/stderr",
                            "MONGODB_EXTRA_ARGS= --port 27017 --replSet " +
                            self.config['configserver']['_id'] + " --configsvr --keyFile /etc/keyfile " + mongod_args,
                            "GOCOVERDIR=/gocoverdir/reports"]
                env_list += [k + "=" + str(v) for k, v in self.extra_environment.items()]
                docker.from_env().containers.run(
                    image='replica_member/local',
                    name=host['host'],
                    hostname=host['host'],
                    detach=True,
                    network='test',
                    environment=env_list,
                    volumes=["fs:/backups","keytabs:/keytabs","gocoverdir:/gocoverdir"]
                )
                if "arbiterOnly" in host:
                    if host['arbiterOnly']:
                        self.__delete_pbm(host['host'])
                conn = conn + host['host'] + ':27017,'
            conn = conn[:-1]
            configdb = conn
            time.sleep(2)
            self.__setup_replicasets(
                self.config['shards'] + [self.config['configserver']])
            self.__setup_authorizations(self.config['shards'])
            Cluster.log("Creating container " + self.config['mongos'])
            docker.from_env().containers.run(
                image='replica_member/local',
                name=self.config['mongos'],
                hostname=self.config['mongos'],
                command='mongos --keyFile=/etc/keyfile --configdb ' +
                configdb + ' --port 27017 --bind_ip 0.0.0.0',
                detach=True,
                network='test'
            )
            Cluster.setup_authorization(self.config['mongos'],self.pbm_mongodb_uri)
            connection = self.connection
            client = pymongo.MongoClient(connection)
            set_default_rw_done = False
            for shard in shards:
                try:
                    result = client.admin.command("addShard", shard)
                    Cluster.log("Adding shard \"" + shard + "\":\n" + str(result))
                except pymongo.errors.OperationFailure as e:
                    errmsg = str(e.details.get("errmsg", ""))
                    if e.code == 96 and "setDefaultRWConcern" in errmsg and not set_default_rw_done:
                        client.admin.command({"setDefaultRWConcern": 1, "defaultWriteConcern": {"w": "majority"}})
                        set_default_rw_done = True
                        result = client.admin.command("addShard", shard)
                        Cluster.log("Adding shard \"" + shard + "\" (after setDefaultRWConcern):\n" + str(result))
                    else:
                        raise
        self.restart_pbm_agents()
        duration = time.time() - start
        Cluster.log("The cluster was prepared in {} seconds".format(duration))

    # setups pbm from default config-file, minio as storage
    def setup_pbm(self, file="/etc/pbm-aws-provider.conf", retries=3):
        host = self.pbm_cli
        n = testinfra.get_host("docker://" + host)
        for attempt in range(retries):
            result = n.run(f'pbm config --file={file} --wait')
            if result.rc == 0:
                Cluster.log("Setup PBM:\n" + result.stdout)
                break
            Cluster.log(f"Setup PBM attempt {attempt + 1} failed (rc={result.rc}):\n"
                f"{result.stdout}\n{result.stderr}")
            if attempt < retries:
                time.sleep(1)
            else:
                raise RuntimeError(f"Setup PBM command failed after {retries} attempts")
        self.wait_pbm_status()

    # pbm --force-resync
    def make_resync(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output('pbm config --force-resync --wait')
        Cluster.log("Resync storage:\n" + result)

    # creates backup based on type (no checking input - it's hack for situation like 'incremental --base')
    def make_backup(self, type=None, allow_fail=False, **kwargs):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        timeout = time.time() + 120
        while True:
            running = self.get_status()['running']
            Cluster.log("Current operation: " + str(running))
            if not running:
                cmd = 'pbm backup --out=json'
                for flag, value in kwargs.items():
                    cmd += f' --{flag}={value}'
                if type:
                    cmd += f' --type={type}'
                start = n.run(cmd)
                if start.rc == 0:
                    name = json.loads(start.stdout)['name']
                    Cluster.log("Backup started")
                    self.cmd_stdout = start.stdout
                    self.cmd_stderr = start.stderr
                    break
                elif "resync" in start.stdout.lower() or "resync" in start.stderr.lower():
                    Cluster.log(f"Resync in progress, retrying: {start.stdout}")
                else:
                    logs = n.check_output("pbm logs -sD -t0")
                    self.cmd_stdout = start.stdout
                    self.cmd_stderr = start.stderr
                    if allow_fail:
                        return False
                    assert False, f"Backup failed: STDOUT={start.stdout} STDERR={start.stderr}\n{logs}"
            if time.time() > timeout:
                assert False, "Timeout for backup start exceeded"
            time.sleep(1)
        timeout = time.time() + 900
        while True:
            status = self.get_status()
            Cluster.log("Current operation: " + str(status['running']))
            if status['backups']['snapshot']:
                for snapshot in status['backups']['snapshot']:
                    if snapshot['name'] == name:
                        if snapshot['status'] == 'done':
                            Cluster.log("Backup found: " + str(snapshot))
                            time.sleep(1) #wait for releasing locks
                            return name
                        elif snapshot['status'] == 'error':
                            logs = n.check_output("pbm logs -sD -t0")
                            assert False, snapshot['error'] + '\n' + logs
            if time.time() > timeout:
                assert False, "Backup timeout exceeded"
            time.sleep(1)

    # restores backup from name, accept extra-args:
    # 1. restart_cluster = bool - restarts cluster after the restore, necessary for physical/incremental
    # 2. make_resync = bool - `pbm --force-resync` after the restore
    # 3. check_pbm_status = bool - check `pbm status` output, raises error if any agent is failed
    def make_restore(self, name, **kwargs):
        # Optional custom restore options (e.g., ["--fallback-enabled=true", "--allow-partly-done=false"])
        restore_opts = kwargs.get('restore_opts', [])

        if self.layout == "sharded":
            self.stop_mongos()
        self.stop_arbiters()
        n = testinfra.get_host("docker://" + self.pbm_cli)
        timeout = time.time() + 60

        while True:
            if not self.get_status()['running']:
                break
            if time.time() > timeout:
                assert False, "Cannot start restore, another operation running"
            time.sleep(1)
        Cluster.log("Restore started")
        timeout=kwargs.get('timeout', 240)
        result = n.run('SSL_CERT_FILE=/etc/nginx-minio/ca.crt timeout ' + str(timeout) +
            ' pbm restore ' + name + ' ' + ' '.join(restore_opts) + ' --wait')
        if "--fallback-enabled=true" in restore_opts and result.rc == 1 and "fallback is applied" in result.stderr.lower():
            # if fallback is enabled and restore fails, PBM should revert the cluster
            # to the state before restore, so just continue execution without raising error
            Cluster.log(result.stdout, result.stderr)
        elif result.rc == 0 and "Error" not in result.stdout:
            Cluster.log(result.stdout, result.stderr)
        elif result.rc == 0 and "Error" in result.stdout:
            assert False, result.stdout + result.stderr
        else:
            # try to catch possible failures if timeout exceeded
            error=''
            for host in self.mongod_hosts:
                try:
                    container = docker.from_env().containers.get(host)
                    get_logs = container.exec_run(
                        'cat /var/lib/mongo/pbm.restore.log', stderr=False)
                    if get_logs.exit_code == 0:
                        Cluster.log(
                            "!!!!Possible failure on {}, file pbm.restore.log was found:".format(host))
                        logs = get_logs.output.decode('utf-8')
                        Cluster.log(logs)
                        if '"s":"F"' in logs:
                            error = logs
                except docker.errors.APIError:
                    pass
            if error:
                assert False, result.stdout + result.stderr + "\n" + error
            else:
                assert False, result.stdout + result.stderr

        restart_cluster=kwargs.get('restart_cluster', False)
        if restart_cluster:
            self.restart()
            self.restart_pbm_agents()
            time.sleep(3)
            self.check_initsync()

        make_resync=kwargs.get('make_resync', True)
        if make_resync:
            self.make_resync()

        check_pbm_status=kwargs.get('check_pbm_status', True)
        if check_pbm_status:
            self.wait_pbm_status()

        if self.layout == "sharded":
            self.start_mongos()
            client = pymongo.MongoClient(self.connection)
            result = client.admin.command("balancerStart")
            Cluster.log("Starting balancer: " + str(result))
            client.close()

    # destroys cluster
    def destroy(self,**kwargs):
        print("\n")
        cleanup=kwargs.get('cleanup_backups', False)
        if cleanup:
            try:
                timeout = time.time() + 30
                self.disable_pitr()
                result=self.exec_pbm_cli("delete-pitr --all --force --yes --wait")
                Cluster.log(result.stdout + result.stderr)
                while True:
                    if not self.get_status()['running'] or time.time() > timeout:
                        break
                result=self.exec_pbm_cli("delete-backup --older-than=0d --force --yes")
                Cluster.log(result.stdout + result.stderr)
                while True:
                    if not self.get_status()['running'] or time.time() > timeout:
                        break
            except AssertionError:
                pass

        for host in self.all_hosts:
            try:
                container = docker.from_env().containers.get(host)
                container.remove(force=True)
                Cluster.log("Container {} was removed".format(host))
            except docker.errors.NotFound:
                pass

    # restarts all containers with mongod sequentially
    def restart(self):
        for host in self.mongod_hosts:
            Cluster.log("Restarting " + host)
            docker.from_env().containers.get(host).restart()
        time.sleep(1)
        self.wait_for_primaries()

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

    # stops mongod's on arbiter hosts
    def stop_arbiters(self):
        for host in self.arbiter_hosts:
            Cluster.log("Stopping arbiter on " + host)
            n = testinfra.get_host("docker://" + host)
            n.check_output("supervisorctl stop mongod")

    # starts mongod's on arbiter hosts
    def start_arbiters(self):
        for host in self.arbiter_hosts:
            Cluster.log("Starting arbiter on " + host)
            n = testinfra.get_host("docker://" + host)
            n.check_output("supervisorctl start mongod")

    # enables PITR
    def enable_pitr(self,**kwargs):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        pitr_extra_args = kwargs.get('pitr_extra_args', "")
        result = n.check_output(
            "pbm config --set pitr.enabled=true --set pitr.compression=none --wait  " + pitr_extra_args)
        Cluster.log("Enabling PITR: " + result)
        self.wait_pitr()

    # disables PITR
    def disable_pitr(self, time_param=None, wait=120):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        if time_param:
            target_time = int(datetime.fromisoformat(time_param).timestamp())
            pitr_end = 0
            Cluster.log('Wait for the chunks with timestamp:' + str(target_time))
            for i in range(wait):
                result = n.check_output("pbm s -s backups -o json")
                backups = json.loads(result)
                if 'backups' in backups and 'pitrChunks' in backups['backups'] and 'pitrChunks' in backups['backups']['pitrChunks']:
                   chunks = []
                   for j in backups['backups']['pitrChunks']['pitrChunks']:
                       chunks.append(j.get('range', {}).get('end', None))
                   pitr_end_cur = max(chunks)
                   # pitr_end_cur = backups['backups']['pitrChunks']['pitrChunks'][0].get('range', {}).get('end', None)
                   Cluster.log('Current chunks end is: ' + str(pitr_end_cur))
                   if pitr_end_cur is not None:
                        pitr_end = pitr_end_cur
                if pitr_end < target_time:
                    time.sleep(1)
                else:
                   Cluster.log("Found necessary chunk with end timestamp: " + str(pitr_end))
                   break
            assert pitr_end >= target_time, "Didn't find the chunks with necessary timestamp " + str(target_time)
        result = n.check_output(
            "pbm config --set pitr.enabled=false --wait")
        Cluster.log("Disabling PITR: " + result)
        self.wait_pitr(enabled=False)

    # executes any pbm command e.g. cluster.exec_pbm_cli("status"), doesn't raise any errors, output from
    # https://testinfra.readthedocs.io/en/latest/modules.html#testinfra.host.Host.run
    def exec_pbm_cli(self, params):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        return n.run("pbm " + params)

    @staticmethod
    def setup_replicaset(replicaset):
        primary = replicaset['members'][0]['host']
        primary = testinfra.get_host("docker://" + primary)
        rs = copy.deepcopy(replicaset)
        rs['members'][0]['priority'] = 1000
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
            result = primary.run("mongo --quiet --eval " + init_rs)
            if result.rc == 0 or 'already initialized' in result.stderr.lower():
                break
            time.sleep(wait_time)
        Cluster.log("Setup replicaset " + json.dumps(rs) + ":\n" + result.stdout.strip())

    def __setup_replicasets(self, replicasets):
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            for rs in replicasets:
                executor.submit(Cluster.setup_replicaset, rs)

    @staticmethod
    def setup_authorization(host,uri):
        primary = testinfra.get_host("docker://" + host)
        Cluster.wait_for_primary(host, "mongodb://127.0.0.1:27017")
        Cluster.log("Setup authorization on " + host)
        Cluster.log("Adding root user on " + host)
        init_root_user = '\'db.getSiblingDB("admin").createUser({ user: "root", pwd: "root", roles: [ "root", "userAdminAnyDatabase", "clusterAdmin" ] });\''
        logs = primary.check_output("mongo --quiet --eval " + init_root_user)
        #Cluster.log(logs)
        Cluster.log("Adding pbm role on " + host)
        init_pbm_role = '\'db.getSiblingDB("admin").createRole({"role": "pbmAnyAction","privileges":[{"resource":{"anyResource":true},"actions":["anyAction"]}],"roles":[]});\''
        logs = primary.check_output(
            "mongo -u root -p root --quiet --eval " + init_pbm_role)
        #Cluster.log(logs)
        Cluster.log("Adding pbm user on " + host)
        init_pbm_user = ('\'db.getSiblingDB("admin").createUser({user:"pbm",pwd:"pbmpass","roles":[' +
                         '{"db":"admin","role":"readWrite","collection":""},' +
                         '{"db":"admin","role":"backup" },' +
                         '{"db":"admin","role":"clusterMonitor" },' +
                         '{"db":"admin","role":"restore" },' +
                         '{"db":"admin","role":"pbmAnyAction" }]});\'')
        init_pbm_t_user = ('\'db.getSiblingDB("admin").createUser({user:"pbm_test",pwd:"pbmpass_test1","roles":[' +
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
        ldap_mongo_grp = ('\'db.getSiblingDB("admin").runCommand({createRole:"cn=readers,ou=groups,dc=example,dc=org",privileges: [],"roles":[' +
                         '{"db":"admin","role":"readWrite","collection":""},' +
                         '{"db":"admin","role":"backup" },' +
                         '{"db":"admin","role":"clusterMonitor" },' +
                         '{"db":"admin","role":"restore" },' +
                         '{"db":"admin","role":"pbmAnyAction" }]});\'')
        logs = primary.check_output(
            "mongo -u root -p root --quiet --eval " + init_pbm_user)
        logs = primary.check_output(
            "mongo -u root -p root --quiet --eval " + init_pbm_t_user)
        Cluster.log(logs)
        if "authMechanism=MONGODB-X509" in uri:
            logs = primary.check_output(
                "mongo -u root -p root --quiet --eval " + x509_pbm_user)
            Cluster.log(logs)
        if "authMechanism=GSSAPI" in uri:
            logs = primary.check_output(
                "mongo -u root -p root --quiet --eval " + krb_pbm_user)
            Cluster.log(logs)
        if "authMechanism=PLAIN" in uri:
            logs = primary.check_output(
                "mongo -u root -p root --quiet --eval " + ldap_mongo_grp)
            Cluster.log(logs)

    def __setup_authorizations(self, replicasets):
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            for rs in replicasets:
                executor.submit(Cluster.setup_authorization,
                                rs['members'][0]['host'], self.pbm_mongodb_uri)
        time.sleep(1)

    @staticmethod
    def wait_for_primary(host, connection):
        n = testinfra.get_host("docker://" + host)
        timeout = time.time() + 60
        Cluster.log("Checking ismaster() on host " + host)
        while True:
            result = n.run(
                "mongo " + connection + " --quiet --eval 'db.hello().isWritablePrimary'")
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
            time.sleep(1)

    def wait_for_primaries(self):
        Cluster.log(self.primary_hosts)
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
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

    def wait_pitr(self,wait=180,enabled=True):
        for i in range(wait):
            if self.check_pitr() == enabled:
                break
            else:
                time.sleep(1)
        assert self.check_pitr() == enabled, self.get_status()['pitr']

    def check_pbm_status(self):
        parsed_result = self.get_status()
        hosts = []
        for replicaset in parsed_result['cluster']:
            for host in replicaset['nodes']:
                if host['role'] != "A":
                    hosts.append(host)
                    assert host['ok']
        assert len(hosts) == len(self.pbm_hosts)

    def wait_pbm_status(self,wait=10):
        for i in range(wait):
            try:
                self.check_pbm_status()
                break
            except AssertionError:
                time.sleep(1)
        self.check_pbm_status()
        Cluster.log("PBM status: " + str(self.get_status()))

    @staticmethod
    def restart_pbm_agent(host):
        Cluster.log("Restarting pbm-agent on host " + host)
        n = testinfra.get_host("docker://" + host)
        n.check_output('supervisorctl restart pbm-agent')
        assert n.supervisor('pbm-agent').is_running

    def restart_pbm_agents(self):
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            for host in self.pbm_hosts:
                executor.submit(Cluster.restart_pbm_agent, host)
        time.sleep(1)

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
        Cluster.log("Downgrading PBM")
        ver = self.get_version()
        Cluster.log("Current PBM version: " + str(ver))
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for host in self.pbm_hosts:
                executor.submit(Cluster.downgrade_single, host, **kwargs)
        time.sleep(5)
        ver = self.get_version()
        Cluster.log("New PBM version: " + str(ver))

    @staticmethod
    def upgrade_single(host):
        n = testinfra.get_host("docker://" + host)
        n.check_output('supervisorctl stop pbm-agent')
        n.check_output('cp -rf /pbm-new/* /usr/bin/')
        n.check_output('supervisorctl start pbm-agent')
        assert n.supervisor('pbm-agent').is_running

    def upgrade(self):
        Cluster.log("Upgrading PBM" )
        ver = self.get_version()
        Cluster.log("Current PBM version: " + str(ver))
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for host in self.pbm_hosts:
                executor.submit(Cluster.upgrade_single, host)
        time.sleep(5)
        ver = self.get_version()
        Cluster.log("New PBM version: " + str(ver))

    def get_logs(self):
        for container in self.pbm_hosts:
            header = "Logs from {name}:".format(name=container)
            Cluster.log(header, '', "=" * len(header))
            try:
                print(docker.from_env().containers.get(
                    container).logs().decode("utf-8", errors="replace"))
            except docker.errors.NotFound:
                pass

    @staticmethod
    def check_initsync_single(host):
        n = testinfra.get_host("docker://" + host)
        result = n.check_output("mongo -u root -p root --quiet --eval \"db.adminCommand( { getLog:'global'} ).log.forEach(x => {print(x)})\" | grep INITSYNC")
        assert "INITSYNC" not in result, 'INITSYNC found on ' + host + ' :\n' + result

    def check_initsync(self):
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            for host in self.pbm_hosts:
                executor.submit(Cluster.check_initsync_single, host)

    @staticmethod
    def log(*args, **kwargs):
        print("[%s]" % (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S'),*args, **kwargs)

    def delete_backup(self, name=None, allow_fail=False, **kwargs):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        cmd = 'pbm delete-backup -y'
        if name:
            cmd += f' {name}'
        for flag, value in kwargs.items():
            cmd += f' --{flag}={value}'
        result = n.run(cmd)
        self.cmd_stdout = result.stdout
        self.cmd_stderr = result.stderr
        if re.search(r"\[done\](?!.*\berror\b)", result.stdout):
            timeout = time.time() + 15
            while True:
                if not self.get_status()['running']:
                    break
                if time.time() > timeout:
                    assert False, 'Backup deletion timeout exceeded'
                time.sleep(0.5)
            Cluster.log(result.stdout)
            return True
        else:
            if allow_fail:
                return False
            assert False, result.stdout

    def external_backup_start(self):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output("pbm backup -t external -o json")
        backup = json.loads(result)['name']
        Cluster.log("External backup name: " + backup)
        timeout = time.time() + 300
        while True:
            status = self.get_status()
            Cluster.log(status['running'])
            if status['running']:
                if status['running']['status'] == "copyReady":
                    break
            if time.time() > timeout:
                assert False
            time.sleep(1)
        result = n.check_output("pbm describe-backup " + backup + " -o json")
        Cluster.log("External backup status: " + result)
        return backup

    def external_backup_copy(self, name):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output("pbm describe-backup " + name + " -o json")
        description=json.loads(result)
        assert "replsets" in description

        os.system("mkdir -p /backups/" + name)
        for rs in description['replsets']:
            Cluster.log("Performing backup for RS " + rs['name'] + " source node: " + rs['node'].split(':')[0])
            n = testinfra.get_host("docker://" + rs['node'].split(':')[0])
            dir = "/backups/" + name + "/" + rs['name'] + "/"
            os.system("mkdir -p " + dir)
            n.check_output("cp -rp /var/lib/mongo/* "  + dir)

    def external_backup_finish(self, name):
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output("pbm backup-finish " + name)
        Cluster.log("External backup finished: " + result)

    def external_restore_start(self, exit=False):
        timeout = time.time() + 60
        while True:
            if not self.get_status()['running']:
                break
            if time.time() > timeout:
                n = testinfra.get_host("docker://" + self.pbm_cli)
                logs = n.check_output("pbm logs -sD -t0")
                assert False, "Cannot start restore, another operation running: " + str(self.get_status()['running']) + "\n" + logs
            time.sleep(1)
        Cluster.log("Restore started")

        if self.layout == "sharded":
            client = pymongo.MongoClient(self.connection)
            result = client.admin.command("balancerStop")
            client.close()
            Cluster.log("Stopping balancer: " + str(result))
            self.stop_mongos()
        self.stop_arbiters()
        n = testinfra.get_host("docker://" + self.pbm_cli)
        if exit:
            result = n.check_output("pbm restore --external --exit")
        else:
            result = n.check_output("pbm restore --external")
        Cluster.log(result)
        restore=result.split()[2]
        Cluster.log("Restore name: " + restore)
        return restore

    def external_restore_copy(self, backup):
        if self.layout == "sharded":
            rsname = self.config['configserver']['_id']
            for node in self.config['configserver']['members']:
                n = testinfra.get_host("docker://" + node['host'])
                n.check_output("rm -rf /var/lib/mongo/*")
                files="/backups/" + backup + "/" + rsname + "/*"
                n.check_output("cp -rp "  + files + " /var/lib/mongo/")
                n.check_output("touch /var/lib/mongo/pbm.restore.log && chown mongodb /var/lib/mongo/pbm.restore.log")
                Cluster.log("Copying files " + files + " to host " + node['host'])

            for shard in self.config['shards']:
                rsname = shard['_id']
                for node in shard['members']:
                    n = testinfra.get_host("docker://" + node['host'])
                    n.check_output("rm -rf /var/lib/mongo/*")
                    files="/backups/" + backup + "/" + rsname + "/*"
                    if node['host'] not in self.arbiter_hosts:
                        n.check_output("cp -rp "  + files + " /var/lib/mongo/")
                        n.check_output("touch /var/lib/mongo/pbm.restore.log && chown mongodb /var/lib/mongo/pbm.restore.log")
                        Cluster.log("Copying files " + files + " to host " + node['host'])
        else:
            rsname = self.config['_id']
            for node in self.config['members']:
                n = testinfra.get_host("docker://" + node['host'])
                n.check_output("rm -rf /var/lib/mongo/*")
                files="/backups/" + backup + "/" + rsname + "/*"
                if node['host'] not in self.arbiter_hosts:
                    n.check_output("cp -rp "  + files + " /var/lib/mongo/")
                    n.check_output("touch /var/lib/mongo/pbm.restore.log && chown mongodb /var/lib/mongo/pbm.restore.log")
                    Cluster.log("Copying files " + files + " to host " + node['host'])

    def external_restore_finish(self, restore, exit=False):
        if exit:
            if self.layout == "sharded":
                rsname = self.config['configserver']['_id']
                for node in self.config['configserver']['members']:
                    n = testinfra.get_host("docker://" + node['host'])
                    pbm_agent_external_command="command=/usr/bin/pbm-agent restore-finish " + restore + " -c /etc/pbm-aws-provider.conf " + " --rs " + rsname + " --node " + node['host'] + ":27017"
                    n.check_output("sed '2d' -i /etc/supervisord.d/pbm-agent-external.ini")
                    n.check_output("echo '" + pbm_agent_external_command + "' >>/etc/supervisord.d/pbm-agent-external.ini")
                    n.check_output("supervisorctl reread")
                    n.check_output("supervisorctl update")
                    result=n.check_output("supervisorctl start pbm-agent-external")
                    Cluster.log("Starting pbm-agent on host " + node['host'] + " :\n" + result)
                for shard in self.config['shards']:
                    rsname = shard['_id']
                    for node in shard['members']:
                        n = testinfra.get_host("docker://" + node['host'])
                        pbm_agent_external_command="command=/usr/bin/pbm-agent restore-finish " + restore + " -c /etc/pbm-aws-provider.conf " + " --rs " + rsname + " --node " + node['host'] + ":27017"
                        n.check_output("sed '2d' -i /etc/supervisord.d/pbm-agent-external.ini")
                        n.check_output("echo '" + pbm_agent_external_command + "' >>/etc/supervisord.d/pbm-agent-external.ini")
                        n.check_output("supervisorctl reread")
                        n.check_output("supervisorctl update")
                        result=n.check_output("supervisorctl start pbm-agent-external")
                        Cluster.log("Starting pbm-agent on host " + node['host'] + " :\n" + result)
            else:
                rsname = self.config['_id']
                for node in self.config['members']:
                    n = testinfra.get_host("docker://" + node['host'])
                    pbm_agent_external_command="command=/usr/bin/pbm-agent restore-finish " + restore + " -c /etc/pbm-aws-provider.conf " + " --rs " + rsname + " --node " + node['host'] + ":27017"
                    n.check_output("sed '2d' -i /etc/supervisord.d/pbm-agent-external.ini")
                    n.check_output("echo '" + pbm_agent_external_command + "' >>/etc/supervisord.d/pbm-agent-external.ini")
                    n.check_output("supervisorctl reread")
                    n.check_output("supervisorctl update")
                    result=n.check_output("supervisorctl start pbm-agent-external")
                    Cluster.log("Starting pbm-agent on host " + node['host'] + " :\n" + result)
        n = testinfra.get_host("docker://" + self.pbm_cli)
        result = n.check_output("pbm restore-finish " + restore + " -c /etc/pbm-aws-provider.conf")
        Cluster.log(result)
        timeout = time.time() + 300
        while True:
            result = n.check_output("pbm describe-restore " + restore + " -c /etc/pbm-aws-provider.conf -o json")
            status = json.loads(result)
            Cluster.log(status['status'])
            if status['status']=='done':
                Cluster.log(status)
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)

        self.restart()
        self.restart_pbm_agents()
        self.make_resync()
        self.wait_pbm_status()
        if self.layout == "sharded":
            self.start_mongos()
            client = pymongo.MongoClient(self.connection)
            result = client.admin.command("balancerStart")
            Cluster.log("Starting balancer: " + str(result))

    @staticmethod
    def psmdb_to_ce(host):
        n=testinfra.get_host("docker://" + host)
        state=n.check_output("mongo --quiet --eval 'db.hello().secondary'")
        Cluster.log("Is mongodb on " + host + " secondary? - " + state)
        n.check_output('supervisorctl stop mongod')
        n.check_output('supervisorctl start mongod-ce')
        Cluster.log("Node " + host + " is now running mongodb CE")
        n.check_output('supervisorctl restart pbm-agent')
        time.sleep(5)
        timeout = time.time() + 30
        while True:
            newstate=n.check_output("mongo --quiet --eval 'db.hello().secondary'")
            Cluster.log("Is mongodb on " + host + " secondary? - " + newstate)
            if newstate == state:
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)
        Cluster.log("Mongodb on " + host + " is in previous state, is secondary: " + newstate)

    @staticmethod
    def ce_to_psmdb(host):
        n=testinfra.get_host("docker://" + host)
        state=n.check_output("mongo --quiet --eval 'db.hello().secondary'")
        Cluster.log("Is mongodb on " + host + " secondary? - " + state)
        n.check_output('supervisorctl stop mongod-ce')
        n.check_output('supervisorctl start mongod')
        Cluster.log("Node " + host + " is now running PSMDB")
        n.check_output('supervisorctl restart pbm-agent')
        time.sleep(5)
        timeout = time.time() + 30
        while True:
            newstate=n.check_output("mongo --quiet --eval 'db.hello().secondary'")
            Cluster.log("Is mongodb on " + host + " secondary? - " + newstate)
            if newstate == state:
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)
        Cluster.log("Mongodb on " + host + " is in previous state, is secondary: " + newstate)

    def network_interruption(self, delay=120, stop_event=None, loss_percent=100):
        client = docker.from_env()
        if self.layout == "replicaset":
            names = [m["host"] for m in self.config["members"]]
        elif self.layout == "sharded":
            names = [m["host"] for m in self.config["configserver"]["members"]]
            for shard in self.config["shards"]:
                names += [m["host"] for m in shard["members"]]
        else:
            return
        containers = [client.containers.get(n) for n in names]
        try:
            for c in containers:
                cmd = f"tc qdisc add dev eth0 root netem loss {loss_percent}%"
                c.exec_run(cmd, privileged=True)
                Cluster.log(f"Simulating {loss_percent}% network loss for {c.name}")
            elapsed = 0
            while elapsed < delay and (not stop_event or not stop_event.is_set()):
                time.sleep(1)
                elapsed += 1
            if stop_event and stop_event.is_set():
                Cluster.log("Network interruption stopped early due to stop signal")
        finally:
            for c in containers:
                c.exec_run("tc qdisc del dev eth0 root netem", privileged=True)
                Cluster.log(f"Restored network connectivity for {c.name}")

    def setup_azurite(self):
        connect_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_name = "test-container"
        try:
            blob_service_client.create_container(container_name)
            Cluster.log(f"Container '{container_name}' created successfully.")
        except ResourceExistsError:
            Cluster.log(f"Container '{container_name}' already exists.")
