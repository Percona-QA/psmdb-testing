import testinfra
import time
import docker
import pymongo
import json
import copy
import concurrent.futures
import os
from datetime import datetime
import re

# class Mongolink for creating/manipulating with single mongolink instance
# name = the name of the container
# src = mongodb uri for -source option
# dst = mongodb uri for -target option

class Mongolink:
    def __init__(self, name, src, dst, **kwargs):
        self.name = name
        self.src = src
        self.dst = dst
        self.mlink_image = kwargs.get('mlink_image', "mlink/local")

    @property
    def container(self):
        client = docker.from_env()
        container = client.containers.get(self.name)
        return container

    def create(self):
        try:
            existing_container = self.container
            Cluster.log(f"Removing existing mlink container '{self.name}'...")
            existing_container.remove(force=True)
        except docker.errors.NotFound:
            pass

        Cluster.log(f"Starting mlink to sync from '{self.src}' → '{self.dst}'...")
        client = docker.from_env()
        container = client.containers.run(
            image=self.mlink_image,
            name=self.name,
            detach=True,
            network="test",
            command=f"mongolink -source {self.src} -target {self.dst} -log-level=debug"
        )
        Cluster.log(f"Mlink '{self.name}' started successfully")

    def destroy(self):
        try:
            self.container.remove(force=True)
        except docker.errors.NotFound:
            pass

    def start(self):
        try:
            exec_result = self.container.exec_run("curl -s -X POST http://localhost:2242/start -d '{}'")

            response = exec_result.output.decode("utf-8").strip()
            status_code = exec_result.exit_code

            if status_code == 0 and response:
                try:
                    json_response = json.loads(response)

                    if json_response.get("ok") is True:
                        Cluster.log("Sync started successfully")
                        return True

                    elif json_response.get("ok") is False:
                        error_msg = json_response.get("error", "Unknown error")
                        Cluster.log(f"Failed to start sync between src and dst cluster: {error_msg}")
                        return False

                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")

            Cluster.log("Failed to start sync between src and dst cluster")
            return False
        except Exception as e:
            Cluster.log(f"Unexpected error: {e}")
            return False


    def finalize(self):
        try:
            exec_result = self.container.exec_run("curl -s -X POST http://localhost:2242/finalize -d '{}'")

            response = exec_result.output.decode("utf-8").strip()
            status_code = exec_result.exit_code

            if status_code == 0 and response:
                try:
                    json_response = json.loads(response)

                    if json_response.get("ok") is True:
                        Cluster.log("Sync finalized successfully")
                        return True

                    elif json_response.get("ok") is False:
                        error_msg = json_response.get("error", "Unknown error")
                        Cluster.log(f"Failed to finalize sync between src and dst cluster: {error_msg}")
                        return False

                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")

            Cluster.log("Failed to finalize sync between src and dst cluster")
            return False
        except Exception as e:
            Cluster.log(f"Unexpected error: {e}")
            return False

    def logs(self):
        try:
            self.container.logs(tail=50).decode("utf-8").strip()
            return logs if logs else "No logs found."

        except docker.errors.NotFound:
            return "Error: mlink container not found."
        except Exception as e:
            return f"Error fetching logs: {e}"


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
    def pml_connection(self):
        if self.layout == "replicaset":
            return "mongodb://pml:test1234@" + self.config['members'][0]['host'] + ":27017/?replicaSet=" + self.config['_id']
        else:
            return "mongodb://pml:test1234@" + self.config['mongos'] + ":27017/"

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

    # configures and starts all docker-containers, creates necessary layout, setups athorization
    def create(self):
        start = time.time()
        Cluster.log("Creating cluster: " + str(self.config))
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
                network='test'
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
        #Cluster.log(logs)
        Cluster.log("Adding pml user on " + host)
        pml_user = ('\'db.getSiblingDB("admin").createUser({user:"pml",pwd:"test1234","roles":[' +
                         '{"db":"admin","role":"backup" },' +
                         '{"db":"admin","role":"clusterMonitor" },' +
                         '{"db":"admin","role":"clusterManager" },' +
                         '{"db":"admin","role":"restore" },' +
                         '{"db":"admin","role":"readWriteAnyDatabase" }]});\'')
        logs = primary.check_output(
            "mongosh -u root -p root --quiet --eval " + pml_user)

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

    @staticmethod
    def compare_database_hashes(db1_container, db2_container):
        query = (
            'db.getMongo().getDBNames().forEach(function(i) { '
            'if (!["admin", "local", "config"].includes(i)) { '
            'var collections = []; '
            'db.getSiblingDB(i).runCommand({ listCollections: 1 }).cursor.firstBatch.forEach(function(coll) { '
            'if (!coll.type || coll.type !== "view") { collections.push(coll.name); } '
            '}); '
            'if (collections.length > 0) { '
            'var result = db.getSiblingDB(i).runCommand({ dbHash: 1, collections: collections }); '
            'print(JSON.stringify({db: i, md5: result.md5, collections: result.collections})); '
            '} else { '
            'print(JSON.stringify({db: i, md5: null, collections: {}})); '
            '}}});'
        )

        def get_db_hashes_and_collections(container):
            exec_result = container.exec_run(f"mongosh -u root -p root --quiet --eval '{query}'")
            response = exec_result.output.decode("utf-8").strip()

            db_hashes = {}
            collection_hashes = {}

            for line in response.split("\n"):
                try:
                    db_info = json.loads(line)
                    db_name = db_info["db"]
                    db_hashes[db_name] = db_info["md5"]

                    for coll, coll_hash in db_info["collections"].items():
                        collection_hashes[f"{db_name}.{coll}"] = coll_hash
                except json.JSONDecodeError:
                    Cluster.log(f"Warning: Skipping invalid JSON line: {line}")

            return db_hashes, collection_hashes

        db1_hashes, db1_collections = get_db_hashes_and_collections(db1_container)
        db2_hashes, db2_collections = get_db_hashes_and_collections(db2_container)

        Cluster.log("Comparing database hashes...")
        mismatched_dbs = []
        for db_name in db1_hashes:
            if db_name not in db2_hashes:
                mismatched_dbs.append(db_name)
                Cluster.log(f"Database '{db_name}' exists in source_DB but not in destination_DB")
            elif db1_hashes[db_name] != db2_hashes[db_name]:
                mismatched_dbs.append(db_name)
                Cluster.log(f"Database '{db_name}' hash mismatch: {db1_hashes[db_name]} != {db2_hashes[db_name]}")

        for db_name in db2_hashes:
            if db_name not in db1_hashes:
                mismatched_dbs.append(db_name)
                Cluster.log(f"Database '{db_name}' exists in destination_DB but not in source_DB")

        Cluster.log("Comparing collection hashes...")
        mismatched_collections = []
        for coll_name in db1_collections:
            if coll_name not in db2_collections:
                mismatched_collections.append(coll_name)
                Cluster.log(f"Collection '{coll_name}' exists in source_DB but not in destination_DB")
            elif db1_collections[coll_name] != db2_collections[coll_name]:
                mismatched_collections.append(coll_name)
                Cluster.log(f"Collection '{coll_name}' hash mismatch: {db1_collections[coll_name]} != {db2_collections[coll_name]}")

        for coll_name in db2_collections:
            if coll_name not in db1_collections:
                mismatched_collections.append(coll_name)
                Cluster.log(f"Collection '{coll_name}' exists in destination_DB but not in source_DB")

        return db1_collections.keys() | db2_collections.keys(), mismatched_dbs, mismatched_collections

    @staticmethod
    def compare_entries_number(db1_container, db2_container):
        query = (
            'db.getMongo().getDBNames().forEach(function(i) { '
            'if (!["admin", "local", "config"].includes(i)) { '
            'var collections = db.getSiblingDB(i).runCommand({ listCollections: 1 }).cursor.firstBatch '
            '.filter(function(coll) { return !coll.type || coll.type !== "view"; }) '
            '.map(function(coll) { return coll.name; }); '
            'collections.forEach(function(coll) { '
            'try { '
            'var count = db.getSiblingDB(i).getCollection(coll).countDocuments({}); '
            'print(JSON.stringify({db: i, collection: coll, count: count})); '
            '} catch (err) {} '
            '});}});'
        )

        def get_collection_counts(container):
            exec_result = container.exec_run(f"mongosh -u root -p root --quiet --eval '{query}'")
            response = exec_result.output.decode("utf-8").strip()

            collection_counts = {}

            for line in response.split("\n"):
                try:
                    count_info = json.loads(line)
                    collection_name = f"{count_info['db']}.{count_info['collection']}"
                    collection_counts[collection_name] = count_info["count"]
                except json.JSONDecodeError:
                    Cluster.log(f"Warning: Skipping invalid JSON line: {line}")

            return collection_counts

        db1_counts = get_collection_counts(db1_container)
        db2_counts = get_collection_counts(db2_container)

        Cluster.log("Comparing collection record counts...")
        mismatched_dbs = []
        mismatched_collections = []

        for coll_name in db1_counts:
            if coll_name not in db2_counts:
                mismatched_collections.append(coll_name)
                Cluster.log(f"Collection '{coll_name}' exists in source_DB but not in destination_DB")
            elif db1_counts[coll_name] != db2_counts[coll_name]:
                mismatched_collections.append(coll_name)
                Cluster.log(f"Collection '{coll_name}' record count mismatch: {db1_counts[coll_name]} != {db2_counts[coll_name]}")

        for coll_name in db2_counts:
            if coll_name not in db1_counts:
                mismatched_collections.append(coll_name)
                Cluster.log(f"Collection '{coll_name}' exists in destination_DB but not in source_DB")

        return db1_counts.keys() | db2_counts.keys(), mismatched_dbs, mismatched_collections

    @staticmethod
    def compare_collection_indexes(db1_container, db2_container, all_collections):
        Cluster.log("Comparing collection indexes...")
        mismatched_indexes = []

        for coll_name in all_collections:
            db1_indexes = Cluster.get_indexes(db1_container, coll_name)
            db2_indexes = Cluster.get_indexes(db2_container, coll_name)

            db1_index_dict = {index["name"]: index for index in db1_indexes if "name" in index}
            db2_index_dict = {index["name"]: index for index in db2_indexes if "name" in index}

            for index_name, index_details in db1_index_dict.items():
                if index_name not in db2_index_dict:
                    mismatched_indexes.append((coll_name, index_name))
                    Cluster.log(f"Collection '{coll_name}': Index '{index_name}' exists in source_DB but not in destination_DB")

            for index_name in db2_index_dict.keys():
                if index_name not in db1_index_dict:
                    mismatched_indexes.append((coll_name, index_name))
                    Cluster.log(f"Collection '{coll_name}': Index '{index_name}' exists in destination_DB but not in source_DB")

            for index_name in set(db1_index_dict.keys()).intersection(db2_index_dict.keys()):
                index1 = db1_index_dict[index_name]
                index2 = db2_index_dict[index_name]

                fields_to_compare = ["key", "unique", "sparse", "partialFilterExpression", "expireAfterSeconds"]
                index1_filtered = {k: index1[k] for k in fields_to_compare if k in index1}
                index2_filtered = {k: index2[k] for k in fields_to_compare if k in index2}

                if index1_filtered != index2_filtered:
                    mismatched_indexes.append((coll_name, index_name))
                    Cluster.log(f"Collection '{coll_name}': Index '{index_name}' differs in structure.")
                    Cluster.log(f"Source_DB: {json.dumps(index1_filtered, indent=2)}")
                    Cluster.log(f"Destination_DB: {json.dumps(index2_filtered, indent=2)}")

        return mismatched_indexes

    @staticmethod
    def get_indexes(container, collection_name):
        db_name, coll_name = collection_name.split(".", 1)

        query = f'db.getSiblingDB("{db_name}").getCollection("{coll_name}").getIndexes()'
        exec_result = container.exec_run(f"mongosh -u root -p root --quiet --json --eval '{query}'")
        response = exec_result.output.decode("utf-8").strip()

        try:
            indexes = json.loads(response)

            def normalize_key(index_key):
                if isinstance(index_key, dict):
                    return {k: normalize_key(v) for k, v in index_key.items()}
                elif isinstance(index_key, list):
                    return [normalize_key(v) for v in index_key]
                elif isinstance(index_key, dict) and "$numberInt" in index_key:
                    return int(index_key["$numberInt"])
                return index_key

            return sorted([
                {
                    "name": index["name"],
                    "key": normalize_key(index["key"]),
                    "unique": index.get("unique", False),
                    "sparse": index.get("sparse", False),
                    "partialFilterExpression": index.get("partialFilterExpression"),
                    "expireAfterSeconds": index.get("expireAfterSeconds")
                }
                for index in indexes if "key" in index and "name" in index
            ], key=lambda x: x["name"])

        except json.JSONDecodeError:
            Cluster.log(f"Error: Unable to parse JSON index response for {collection_name}")
            Cluster.log(f"Raw response: {response}")
            return []

    @staticmethod
    def compare_data_rs(db1, db2):
        client = docker.from_env()

        db1_container = client.containers.get(db1.entrypoint)
        db2_container = client.containers.get(db2.entrypoint)

        all_coll_hash, mismatch_dbs_hash, mismatch_coll_hash = Cluster.compare_database_hashes(db1_container, db2_container)
        all_coll_count, mismatch_dbs_count, mismatch_coll_count = Cluster.compare_entries_number(db1_container, db2_container)

        mismatch_indexes = Cluster.compare_collection_indexes(db1_container, db2_container, all_coll_hash)

        if not any([mismatch_dbs_hash, mismatch_coll_hash, mismatch_dbs_count, mismatch_coll_count, mismatch_indexes]):
            Cluster.log("Data and indexes are consistent between source and destination databases")
            return True
        else:
            Cluster.log("Mismatched databases, collections, or indexes found")
        return False

    @staticmethod
    def compare_data_sharded(db1, db2):
        client = docker.from_env()

        db1_container = client.containers.get(db1.entrypoint)
        db2_container = client.containers.get(db2.entrypoint)

        all_collections, mismatched_dbs, mismatched_collections = Cluster.compare_entries_number(db1_container, db2_container)

        mismatched_indexes = Cluster.compare_collection_indexes(db1_container, db2_container, all_collections)

        if not mismatched_dbs and not mismatched_collections and not mismatched_indexes:
            Cluster.log("Data and indexes are consistent between source and destination databases")
            return True
        else:
            Cluster.log("Mismatched databases, collections, or indexes found")
        return False
