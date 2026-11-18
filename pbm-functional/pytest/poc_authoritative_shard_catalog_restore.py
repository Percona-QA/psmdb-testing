"""Diagnose authoritative shard catalog recovery after a logical PBM restore."""

import fnmatch
import shutil
import time
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from bson import BSON, decode_file_iter, json_util
from bson.timestamp import Timestamp
from bson.int64 import Int64

import docker
import pymongo
import pytest
from pymongo.errors import OperationFailure
from pymongo.write_concern import WriteConcern

from cluster import Cluster


def grant_pbm_catalog_role(members):
    # The PoC calls _shardsvrCommitCreateDatabaseMetadata and
    # metadata commands as PBM. Both require the cluster-level internal
    # action, which the agent's ordinary roles do not explicitly provide.
    # Provision it with the disposable cluster so the role is present in backup
    # and is already available when the post-restore PoC runs.
    for nodes in members.values():
        primary = next(
            node for node in nodes if node.admin.command("hello")["isWritablePrimary"]
        )
        primary.admin.command(
            "createRole", "restoreCatalogInternal",
            privileges=[{"resource": {"cluster": True}, "actions": ["internal"]}],
            roles=[], writeConcern={"w": "majority"},
        )
        primary.admin.command(
            "grantRolesToUser", "pbm",
            roles=[{"role": "restoreCatalogInternal", "db": "admin"}],
            writeConcern={"w": "majority"},
        )


@pytest.fixture
def restore_cluster(request):
    shards = [
        {
            "_id": f"rs{shard}",
            "members": [
                {"host": f"rs{shard}0{member}"} for member in range(1, 4)
            ],
        }
        for shard in (1, 2)
    ]
    cluster = Cluster({
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
        "shards": shards,
    })
    storage = f"/backups/authrestore-{uuid4().hex[:8]}"
    try:
        cluster.create()
        config_server = docker.from_env().containers.get(cluster.pbm_cli)
        storage_config = "/tmp/authoritative-restore-fs.conf"
        result = config_server.exec_run([
            "sh", "-ec",
            'mkdir -m 0777 "$1"; sed "s|path: /backups|path: $1|" /etc/pbm-fs.conf > "$2"',
            "setup-fs", storage, storage_config,
        ], user="root")
        assert result.exit_code == 0, result.output
        cluster.setup_pbm(storage_config)
        with ExitStack() as stack:
            router = stack.enter_context(pymongo.MongoClient(
                cluster.connection, serverSelectionTimeoutMS=5000, timeoutMS=15000
            ))
            members = {}
            for shard in shards:
                members[shard["_id"]] = [
                    stack.enter_context(pymongo.MongoClient(
                        f"mongodb://root:root@{member['host']}:27017/?authSource=admin",
                        directConnection=True,
                        read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED,
                        serverSelectionTimeoutMS=5000,
                        timeoutMS=15000,
                    ))
                    for member in shard["members"]
                ]
            grant_pbm_catalog_role(members)
            assert all(
                node.admin.command("getParameter", featureFlagAuthoritativeShardsCRUD=1)
                ["featureFlagAuthoritativeShardsCRUD"]["value"]
                for nodes in members.values() for node in nodes
            ), "The test needs authoritative shard CRUD enabled"
            process_ids = {
                node.address: node.admin.command("hello")["topologyVersion"]["processId"]
                for nodes in members.values() for node in nodes
            }

            try:
                yield cluster, router, members, process_ids, storage
            finally:
                for nodes in members.values():
                    for node in nodes:
                        node.close()
    finally:
        try:
            cluster.destroy()
        finally:
            if Path(storage).exists():
                shutil.rmtree(storage)


def seed_data(router, members, timeseries=False):
    router.admin.command("balancerStop")
    router.admin.command("enableSharding", "catalog_restore", primaryShard="rs1")
    router.admin.command("shardCollection", "catalog_restore.items", key={"_id": 1})
    for boundary in (-100, 0, 100):
        router.admin.command("split", "catalog_restore.items", middle={"_id": boundary})
    router.admin.command(
        "moveChunk", "catalog_restore.items", find={"_id": 1},
        to="rs2", _waitForDelete=True,
    )
    items = router.catalog_restore.items
    documents = [{"_id": value} for value in (-200, -1, 1, 200)]
    items.insert_many(documents)
    items.create_index("value", name="value_index")
    router.admin.command("enableSharding", "restore_version", primaryShard="rs1")
    router.restore_version.items.insert_one({"_id": "backup"})
    # Exercise ordinary collections in databases owned by both primary shards.
    router.admin.command("enableSharding", "restore_rs2", primaryShard="rs2")
    for database in ("catalog_restore", "restore_rs2"):
        router[database].create_collection(
            "ordinary",
            validator={"value": {"$type": "number"}},
            validationLevel="strict",
            validationAction="error",
        )
        router[database].ordinary.create_index("value", name="ordinary_value_index")
        router[database].ordinary.insert_many([
            {"_id": "first", "value": 1},
            {"_id": "second", "value": 2},
        ])
        router[database].command(
            "create", "ordinary_view", viewOn="ordinary",
            pipeline=[{"$match": {"value": {"$gte": 2}}}],
        )
    source_catalog = members["rs1"][0].config["shard.catalog.databases"]
    database_at_backup = source_catalog.find_one({"_id": "restore_version"})
    assert database_at_backup is not None
    catalog_names = ("databases", "collections", "chunks")
    db = router.catalog_restore
    if timeseries:
        db.create_collection("measurements", timeseries={"timeField": "time", "metaField": "sensor"})
        measurement = {"_id": 1, "time": datetime(2026, 1, 1), "sensor": "a", "value": 10}
        db.measurements.insert_one(measurement)
    db.command("create", "positive_items", viewOn="items", pipeline=[{"$match": {"_id": {"$gt": 0}}}])
    catalog_at_backup = {
        shard_id: {
            name: list(nodes[0].config[f"shard.catalog.{name}"].find())
            for name in catalog_names
        }
        for shard_id, nodes in members.items()
    }
    return catalog_at_backup, documents


def perturb_metadata(router, members, catalog):
    router.admin.command(
        "moveChunk", "catalog_restore.items", find={"_id": 1},
        to="rs1", _waitForDelete=True,
    )
    router.drop_database("restore_version")
    router.admin.command("enableSharding", "restore_version", primaryShard="rs1")
    router.restore_version.items.insert_one({"_id": "after backup"})
    # Give the second primary shard a stale database incarnation as well.
    router.drop_database("restore_rs2")
    router.admin.command("enableSharding", "restore_rs2", primaryShard="rs2")
    router.restore_rs2.ordinary.insert_one({"_id": "after backup"})
    original = next(doc for doc in catalog["rs1"]["databases"] if doc["_id"] == "restore_version")
    assert members["rs1"][0].config["shard.catalog.databases"].find_one(
        {"_id": "restore_version"}
    ) != original



def restore_authoritative_catalog_poc(members, catalog_snapshot, global_collections=None):
    """Restore shard-local catalog documents and synchronize database metadata caches.

    The saved catalog belongs to the same shard IDs as the destination. In these
    PITR tests, catalog metadata stays unchanged between backup and the target time.
    Cluster setup supplies PBM with the internal privilege required by the commands.
    """

    # Restore the durable shard-local catalog.
    # PBM currently restores the global catalog on the config server but omits
    # config.shard.catalog.{databases,collections,chunks} on individual shards.
    # Reconcile each shard with its saved snapshot, including removing documents
    # absent from that snapshot. Majority writes replicate these ordinary catalog
    # writes, but do not rebuild the in-memory database metadata maintained by
    # dedicated metadata oplog events. The next phase populates that state.
    for shard_id, nodes in members.items():
        primary = next(
            node for node in nodes if node.admin.command("hello")["isWritablePrimary"]
        )
        for name, expected in catalog_snapshot[shard_id].items():
            catalog = primary.config[f"shard.catalog.{name}"].with_options(
                write_concern=WriteConcern(w="majority")
            )
            present = {doc["_id"]: doc for doc in catalog.find()}
            expected_by_id = {doc["_id"]: doc for doc in expected}
            if present == expected_by_id:
                continue
            Cluster.log(f"{shard_id}: restoring local shard catalog {name} manually")
            extra_ids = list(present.keys() - expected_by_id.keys())
            if extra_ids:
                catalog.delete_many({"_id": {"$in": extra_ids}})
            for key, doc in expected_by_id.items():
                if present.get(key) != doc:
                    catalog.replace_one({"_id": key}, doc, upsert=True)
            assert {doc["_id"]: doc for doc in catalog.find()} == expected_by_id

    # Apply the restored metadata through existing internal server commands.
    # _shardsvrCommitCreateDatabaseMetadata upserts the database's local catalog
    # document, emits createDatabaseMetadata into the destination oplog, and sets
    # DatabaseShardingState on the primary. Applying that event sets the same
    # database metadata on secondaries. This makes the restored dbVersion available
    # to routed requests after direct writes left the database cache stale or empty.
    #
    # _shardsvrFetchCollMetadata reads collection metadata and owned chunks from
    # the restored config server and persists them in the shard-local catalog.
    # Its necessity was checked by removing the call: all six scenarios without
    # time-series then failed during CRUD with "No progress was made executing
    # write ops". Restoring the call made both snapshot control tests pass.
    # Keep this step even after manually restoring the local catalog. The exact
    # mechanism by which the fetch repairs this write failure remains unverified.
    # This implementation intentionally skips installing or invalidating in-memory
    # collection metadata. Successful test cases therefore do not establish that
    # arbitrary stale collection caches are repaired.
    #
    # Both commands require a retryable-write context, supplied below by a session
    # and txnNumber. The database commit command takes its database from $db.
    failures = []
    for shard_id, nodes in members.items():
        primary = next(node for node in nodes if node.admin.command("hello")["isWritablePrimary"])
        probes = [
            {
                "_shardsvrCommitCreateDatabaseMetadata": doc["_id"],
                "dbVersion": doc["version"],
            }
            for doc in primary.config["shard.catalog.databases"].find()
        ]
        database_primaries = {
            doc["_id"]: doc["primary"]
            for shard_catalog in catalog_snapshot.values()
            for doc in shard_catalog["databases"]
        }
        for doc in (global_collections if global_collections is not None
                    else catalog_snapshot[shard_id]["collections"]):
            namespace = doc["_id"]
            database = namespace.split(".", 1)[0]
            if database not in database_primaries:
                continue
            probes.append({
                "_shardsvrFetchCollMetadata": namespace,
                "primaryShardId": database_primaries[database],
            })
        pbm_client = pymongo.MongoClient(
            f"mongodb://pbm:pbmpass@{primary.address[0]}:27017/?authSource=admin",
            directConnection=True, timeoutMS=30000,
        )
        with pbm_client, pbm_client.start_session() as session:
            for number, command in enumerate(probes):
                name = next(iter(command))
                target = command[name]
                try:
                    response = pbm_client[command.get("_shardsvrCommitCreateDatabaseMetadata", "admin")].command(
                        command, session=session, txnNumber=Int64(number),
                        writeConcern={"w": "majority"},
                    )
                    Cluster.log(f"{shard_id}: {name} target={target} succeeded: {response}")
                except OperationFailure as exc:
                    Cluster.log(f"{shard_id}: {name} target={target} rejected: {exc}")
                    failures.append(f"{shard_id}: {name} target={target}: {exc}")
        if not failures:
            # Majority acknowledgement can precede application on a secondary.
            # Wait for every member to apply through the commands' final oplog entry
            # before the tests read restored data directly from those members.
            through = primary.local["oplog.rs"].find_one(sort=[("$natural", -1)])["ts"]
            deadline = time.monotonic() + 30
            for node in nodes:
                while node.admin.command("replSetGetStatus")["optimes"]["appliedOpTime"]["ts"] < through:
                    assert time.monotonic() < deadline, f"{node.address} did not catch up"
                    time.sleep(0.5)
    assert not failures, "\n".join(failures)


def filter_replay_operation(operation, internal_events, namespaces, replica_set):
    """Filter nested operations while retaining the source transaction envelope.

    mongorestore owns transaction buffering, prepare/commit handling and aborts.
    Preserve lsid, txnNumber, prevOpTime, partialTxn, prepare and commit timestamps;
    flattening applyOps here would apply uncommitted or aborted writes prematurely.
    The original timestamp order is retained by the caller across all source slices.
    """
    command = operation.get("o", {}) if operation["op"] == "c" else {}
    if "applyOps" in command:
        filtered = []
        for child in command["applyOps"]:
            selected = filter_replay_operation(child, internal_events, namespaces, replica_set)
            if selected is not None:
                filtered.append(selected)
        # Retain even an empty wrapper: it can terminate a split transaction or
        # carry prepare. Commit and abort envelopes must reach the tools buffer.
        result = dict(operation)
        result["o"] = dict(command, applyOps=filtered)
        return result
    if "commitTransaction" in command or "abortTransaction" in command:
        return operation

    namespace = operation.get("ns", "")
    database, _, collection = namespace.partition(".")
    if database in ("admin", "local") or collection.startswith("pbm") or namespace in ("config.version", "config.shards"):
        return None
    # Match the logical backup's config-server collection selection. Operational
    # collections such as placementHistory are regenerated on the destination and
    # have destination UUIDs; replaying their source UUIDs would corrupt targeting.
    if database == "config" and collection not in (
        "chunks", "collections", "databases", "settings", "shards", "tags", "version", "$cmd"
    ):
        return None
    if namespaces and database == "config":
        return None
    if command:
        name = next(iter(command))
        if name in internal_events:
            Cluster.log(f"{replica_set}: defer {name} on {namespace} to catalog finalization")
            return None
        if namespaces:
            affected = f"{database}.{command[name]}"
            if name == "renameCollection":
                affected = command[name]
                assert any(fnmatch.fnmatchcase(command["to"], pattern) for pattern in namespaces), (
                    "Renaming across the namespace filter is not supported"
                )
            if not any(fnmatch.fnmatchcase(affected, pattern) for pattern in namespaces):
                return None
    elif namespaces and not any(fnmatch.fnmatchcase(namespace, pattern) for pattern in namespaces):
        return None
    if command and next(iter(command)) == "create" and "o2" in operation:
        # Match PBM 7d7d0aa: source catalogId/ident/idIndexIdent are physical
        # storage metadata. Keep logical create and UUID but allocate fresh idents
        # on the destination, avoiding collisions with timestamped pending drops.
        operation = dict(operation)
        operation.pop("o2")
    if collection == "system.views" and "ui" in operation:
        # mongorestore recreates views from logical metadata, so system.views gets
        # a destination UUID even with --preserveUUID. Replay its changes by the
        # preserved database namespace, not the source system.views UUID.
        operation = dict(operation)
        operation.pop("ui")
    return operation


def supplement_timeseries_backup(cluster, members, storage, backup):
    """Model missing PBM rawData dumping for the quiescent time-series tests.

    PBM's original backup is retained for ordinary data. No writes occur between
    it and this supplemental read in these tests. This is not a production solution
    for an actively written backup or a replacement for snapshot/oplog coordination.
    Both PBM metadata formats are updated to describe the captured raw BSON data.
    """
    for shard_id, nodes in members.items():
        source = Path(storage) / backup / shard_id
        manifest_path = source / "metadata.json"
        manifest = json_util.loads(manifest_path.read_text())
        primary = next(node for node in nodes if node.admin.command("hello")["isWritablePrimary"])
        for entry in manifest["namespaces"]:
            if entry.get("type") != "timeseries":
                continue
            database = entry.get("db", entry.get("database"))
            collection = entry["collection"]
            response = primary[database].command({
                "find": collection, "filter": {}, "rawData": True, "batchSize": 1000
            })
            documents = response["cursor"]["firstBatch"]
            cursor_id = response["cursor"]["id"]
            while cursor_id:
                response = primary[database].command({"getMore": cursor_id, "collection": collection})
                documents.extend(response["cursor"]["nextBatch"])
                cursor_id = response["cursor"]["id"]
            assert documents, f"Expected raw time-series data for {database}.{collection}"
            data = b"".join(BSON.encode(document) for document in documents)
            (source / f"{database}.{collection}").write_bytes(data)
            # Match Go hash/crc64 with the ECMA table and its complemented state.
            checksum = (1 << 64) - 1
            for byte in data:
                checksum ^= byte
                for bit in range(8):
                    checksum = (checksum >> 1) ^ (0xC96C5795D7870F42 if checksum & 1 else 0)
            checksum ^= (1 << 64) - 1
            if checksum >= 1 << 63:
                checksum -= 1 << 64
            entry["size"] = Int64(len(data))
            entry["crc"] = Int64(checksum)
            v2_path = source / "meta.pbm"
            if v2_path.exists():
                v2 = BSON(v2_path.read_bytes()).decode()
                for specification in v2["specifications"]:
                    if specification["db"] == database and specification["name"] == collection:
                        specification["size"] = Int64(len(data))
                        specification["crc"] = Int64(checksum)
                v2_path.write_bytes(BSON.encode(v2))
            Cluster.log(f"{shard_id}: supplemented {database}.{collection}: {len(documents)} raw buckets, {len(data)} bytes")
        manifest_path.write_text(json_util.dumps(manifest, json_options=json_util.CANONICAL_JSON_OPTIONS))


def simulate_agent_restore(cluster, router, members, storage, backup, catalog_snapshot,
                           target=None, namespaces=None):
    """Experimental logical agent flow using installed mongorestore, without PBM replay.

    The local-catalog snapshot remains supplied by the test. This does not implement
    production catalog reconstruction. Transaction replay is delegated to mongorestore.
    Internal cache events are deferred in this experiment, rather than forwarded to
    applyOps (which rejects them). Unsupported events still fail mongorestore.
    """
    # 1. Freeze placement before touching the global and shard-local catalogs.
    router.admin.command("balancerStop")
    cluster.stop_mongos()
    root = Path(storage)
    # 2. Match PBM fullRestoreDBCleanup/selRestoreDBCleanup. These internal DDL
    # commands perform a cluster-wide drop from the database's primary shard,
    # removing metadata from the CSRS and all shards. A local dropDatabase on each
    # replica set does not perform that metadata cleanup.
    replica_sets = [cluster.config["configserver"], *cluster.config["shards"]]
    databases_to_restore = set()
    for replica_set in replica_sets:
        archive = json_util.loads(
            (root / backup / replica_set["_id"] / "metadata.json").read_text()
        )
        databases_to_restore.update(
            entry.get("db", entry.get("database")) for entry in archive["namespaces"]
        )
    with pymongo.MongoClient(
        f"mongodb://root:root@{cluster.pbm_cli}:27017/?authSource=admin",
        directConnection=True,
    ) as config_client:
        selected = namespaces or [f"{name}.*" for name in sorted(databases_to_restore)]
        dropped_databases = set()
        for namespace in selected:
            database, _, collection = namespace.partition(".")
            if database in ("admin", "config", "local") or database in dropped_databases:
                continue
            db_metadata = config_client.config.databases.find_one({"_id": database})
            if db_metadata is None:
                continue
            nodes = members[db_metadata["primary"]]
            primary = next(node for node in nodes if node.admin.command("hello")["isWritablePrimary"])
            with pymongo.MongoClient(
                f"mongodb://pbm:pbmpass@{primary.address[0]}:27017/?authSource=admin",
                directConnection=True, timeoutMS=120000,
            ) as agent:
                if collection in ("", "*"):
                    command = {"_shardsvrDropDatabase": 1}
                    dropped_databases.add(database)
                else:
                    assert not any(char in collection for char in "*?"), namespace
                    command = {"_shardsvrDropCollection": collection}
                command["databaseVersion"] = db_metadata["version"]
                command["writeConcern"] = {"w": "majority"}
                Cluster.log(f"{db_metadata['primary']}: cleanup {namespace} using {next(iter(command))}")
                agent[database].command(command)
    metadata = json_util.loads((root / f"{backup}.pbm.json").read_text())
    assert metadata["compression"] in ("none", ""), metadata["compression"]
    end = Timestamp(metadata["last_write_ts"]["T"], metadata["last_write_ts"]["I"])
    if target:
        end = Timestamp(int(datetime.strptime(target, "%Y-%m-%dT%H:%M:%S").timestamp()), 0)
    start = Timestamp(metadata["first_write_ts"]["T"], metadata["first_write_ts"]["I"])
    scratch = root / "manual_restore"
    scratch.mkdir()
    replica_sets = [cluster.config["configserver"], *cluster.config["shards"]]
    internal_events = {
        "createDatabaseMetadata", "dropDatabaseMetadata", "invalidateCollectionMetadata",
        "invalidateAllCollectionMetadata", "invalidateAllDatabaseMetadata",
        "updateCollectionMetadata", "setAllowChunkOperations",
    }
    try:
        # 3. Reconstitute mongodump directory metadata from PBM's uncompressed split
        # archive. Restore the config server first, then each shard, preserving UUIDs.
        # Keep destination credentials and PBM bookkeeping out of this experiment.
        for replica_set in replica_sets:
            rs = replica_set["_id"]
            source = root / backup / rs
            archive = json_util.loads((source / "metadata.json").read_text())
            dump = scratch / rs / "dump"
            dump.mkdir(parents=True)
            for entry in archive["namespaces"]:
                database = entry.get("db", entry.get("database"))
                collection = entry["collection"]
                namespace = f"{database}.{collection}"
                if database in ("admin", "local") or collection.startswith("pbm") or namespace in ("config.version", "config.shards"):
                    continue
                # Partial restores retain global database versions and unrelated
                # namespaces. Selected global collection metadata is merged below.
                if namespaces and database == "config":
                    continue
                if namespaces and database != "config" and not any(
                    fnmatch.fnmatchcase(namespace, pattern) for pattern in namespaces
                ):
                    continue
                directory = dump / database
                directory.mkdir(exist_ok=True)
                options_metadata = json_util.loads(entry["metadata"])
                if entry.get("type") == "timeseries":
                    # For 9.0 viewless time-series, applyOps otherwise synthesizes
                    # a legacy _id index because listCollections omits clusteredIndex.
                    options_metadata["options"]["clusteredIndex"] = True
                (directory / f"{collection}.metadata.json").write_text(json_util.dumps(options_metadata))
                data = source / namespace
                if data.exists():
                    shutil.copyfile(data, directory / f"{collection}.bson")
                elif entry.get("type") != "view":
                    (directory / f"{collection}.bson").touch()
            host = replica_set["members"][0]["host"]
            if namespaces and replica_set is replica_sets[0]:
                # Only restore selected collection/chunk/tag documents on CSRS.
                # The database version and unrelated records must retain their
                # destination values when other namespaces are excluded.
                with pymongo.MongoClient(
                    f"mongodb://root:root@{host}:27017/?authSource=admin",
                    directConnection=True,
                ) as config_client:
                    source_collections = []
                    with (source / "config.collections").open("rb") as stream:
                        source_collections = list(decode_file_iter(stream))
                    selected = [doc for doc in source_collections if doc["_id"] in namespaces]
                    uuids = {doc["uuid"] for doc in selected}
                    for collection in ("collections", "chunks", "tags"):
                        data_file = source / f"config.{collection}"
                        documents = []
                        if data_file.exists():
                            with data_file.open("rb") as stream:
                                documents = list(decode_file_iter(stream))
                        def belongs(doc):
                            if collection == "collections":
                                return doc["_id"] in namespaces
                            if collection == "chunks":
                                return doc["uuid"] in uuids
                            return doc.get("ns") in namespaces
                        target_collection = config_client.config[collection].with_options(
                            write_concern=WriteConcern(w="majority")
                        )
                        for doc in list(target_collection.find()):
                            if belongs(doc):
                                target_collection.delete_one({"_id": doc["_id"]})
                        for doc in documents:
                            if belongs(doc):
                                target_collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            container = docker.from_env().containers.get(host)
            uri = "mongodb://root:root@127.0.0.1:27017/?authSource=admin&directConnection=true"
            command = ["mongorestore", "--uri", uri, "--drop", "--preserveUUID",
                       "--stopOnError", "--numParallelCollections=1", f"--dir={dump}"]
            result = container.exec_run(command)
            Cluster.log(f"{rs}: snapshot mongorestore: {result.output.decode()}")
            assert result.exit_code == 0, result.output.decode()

        # 5. Replay snapshot consistency oplog and, for PITR, subsequent slices up to
        # the target. Sort and deduplicate by source timestamp per replica set.
        # This deliberately defers authoritative metadata cache events to step 4;
        # it does not drop arbitrary unknown commands to hide replay failures.
        for replica_set in replica_sets:
            rs = replica_set["_id"]
            sources = list((root / backup / rs / "oplog").rglob("*"))
            if target:
                sources += list((root / "pbmPitr" / rs).rglob("*"))
            operations = {}
            for path in sources:
                if not path.is_file():
                    continue
                with path.open("rb") as stream:
                    for operation in decode_file_iter(stream):
                        timestamp = operation["ts"]
                        if start <= timestamp <= end:
                            operations[timestamp] = operation
            replay = scratch / rs / "oplog.bson"
            with replay.open("wb") as stream:
                for timestamp, operation in sorted(operations.items()):
                    selected = filter_replay_operation(operation, internal_events, namespaces, rs)
                    if selected is not None:
                        stream.write(BSON.encode(selected))
            empty = scratch / rs / "empty"
            empty.mkdir()
            host = replica_set["members"][0]["host"]
            result = docker.from_env().containers.get(host).exec_run([
                "mongorestore", "--uri",
                "mongodb://pbm:pbmpass@127.0.0.1:27017/?authSource=admin&directConnection=true",
                "--verbose=3", "--oplogReplay", f"--oplogFile={replay}", f"--dir={empty}",
                "--preserveUUID", "--drop", "--stopOnError",
            ])
            Cluster.log(f"{rs}: oplog mongorestore: {result.output.decode()}")
            if result.exit_code != 0:
                Cluster.log(docker.from_env().containers.get(host).logs(tail=300).decode())
            assert result.exit_code == 0, result.output.decode()

        # 6. Finalize authoritative metadata after all oplog operations have applied.
        # This control tests whether pre-replay invalidation caused StaleConfig.
        # Database creation during PITR is reflected in the restored global
        # catalog, not the backup-time local snapshot. Install those final database
        # versions and fetch all final collection namespaces, including new ones.
        with pymongo.MongoClient(
            f"mongodb://root:root@{cluster.pbm_cli}:27017/?authSource=admin",
            directConnection=True,
        ) as config_client:
            databases = list(config_client.config.databases.find())
            collections = list(config_client.config.collections.find())
        final_catalog = {
            shard_id: dict(saved, databases=[doc for doc in databases if doc["primary"] == shard_id])
            for shard_id, saved in catalog_snapshot.items()
        }
        restore_authoritative_catalog_poc(members, final_catalog, global_collections=collections)
        # Direct logical writes can rewind chunk versions while the routing cache
        # still holds the pre-restore version. Clear that separate per-process cache.
        for nodes in members.values():
            for node in nodes:
                node.admin.command("flushRouterConfig", 1)
        for member in cluster.config["configserver"]["members"]:
            with pymongo.MongoClient(
                f"mongodb://root:root@{member['host']}:27017/?authSource=admin",
                directConnection=True,
            ) as node:
                node.admin.command("flushRouterConfig", 1)
        cluster.start_mongos()
        router.admin.command("balancerStart")
    finally:
        shutil.rmtree(scratch)


def verify_restored(cluster, router, members, process_ids, documents):
    crud_checks(router)
    assert router.restore_version.items.find_one({"_id": "backup"}) == {"_id": "backup"}
    assert list(router.catalog_restore.items.find().sort("_id")) == documents
    assert list(router.catalog_restore.positive_items.find().sort("_id")) == [
        doc for doc in documents if doc["_id"] > 0
    ]
    expected_ordinary = [{"_id": "first", "value": 1}, {"_id": "second", "value": 2}]
    for database, owner in (("catalog_restore", "rs1"), ("restore_rs2", "rs2")):
        assert router.config.databases.find_one({"_id": database})["primary"] == owner
        assert list(router[database].ordinary.find().sort("_id")) == expected_ordinary
        assert list(router[database].ordinary_view.find()) == [expected_ordinary[1]]
        definitions = {
            entry["name"]: entry for entry in router[database].list_collections()
        }
        assert definitions["ordinary"]["type"] == "collection"
        ordinary_options = definitions["ordinary"]["options"]
        assert ordinary_options["validator"] == {"value": {"$type": "number"}}
        assert ordinary_options["validationLevel"] == "strict"
        assert ordinary_options["validationAction"] == "error"
        view = definitions["ordinary_view"]
        assert view["type"] == "view"
        assert view["options"]["viewOn"] == "ordinary"
        assert view["options"]["pipeline"] == [{"$match": {"value": {"$gte": 2}}}]
        assert router[database].ordinary.index_information()["ordinary_value_index"]["key"] == [("value", 1)]

    definition = router.config.collections.find_one({"_id": "catalog_restore.items"})
    assert definition["key"] == {"_id": 1}
    assert router.config.chunks.count_documents({"uuid": definition["uuid"]}) >= 4
    definitions = {
        entry["name"]: entry for entry in router.catalog_restore.list_collections()
    }
    assert definitions["items"]["type"] == "collection"
    view = definitions["positive_items"]
    assert view["type"] == "view"
    assert view["options"]["viewOn"] == "items"
    assert view["options"]["pipeline"] == [{"$match": {"_id": {"$gt": 0}}}]
    assert router.catalog_restore.items.index_information()["value_index"]["key"] == [("value", 1)]
    secondary_items = router.catalog_restore.items.with_options(
        read_preference=pymongo.ReadPreference.SECONDARY
    )
    assert list(secondary_items.find().sort("_id")) == documents
    # Process identity is diagnostic only; all data/schema checks use mongos.
    for nodes in members.values():
        for node in nodes:
            assert node.admin.command("hello")["topologyVersion"]["processId"] == process_ids[node.address]


def crud_checks(router):
    for collection in (
        router.catalog_restore.items,
        router.restore_version.items,
        router.catalog_restore.ordinary,
        router.restore_rs2.ordinary,
    ):
        collection = collection.with_options(write_concern=WriteConcern(w="majority"))
        for key in (-10, 10):
            collection.insert_one({"_id": key, "value": 0})
            assert collection.find_one({"_id": key}) == {"_id": key, "value": 0}
            assert collection.update_one({"_id": key}, {"$set": {"value": 1}}).modified_count == 1
            assert collection.find_one({"_id": key})["value"] == 1
            assert collection.delete_one({"_id": key}).deleted_count == 1
            assert collection.find_one({"_id": key}) is None


def ddl_checks(router):
    """Exercise DDL against restored collections and newly created namespaces."""
    db = router.catalog_restore
    index = db.items.create_index("ddl_value", name="restore_ddl_index")
    assert index in db.items.index_information()
    db.items.drop_index(index)
    assert index not in db.items.index_information()

    # collMod must resolve the restored view and its source collection.
    db.command("collMod", "positive_items", viewOn="items", pipeline=[{"$match": {"_id": {"$gt": 0}}}])
    db.create_collection("ddl_collection")
    db.ddl_collection.insert_one({"_id": 1})
    db.ddl_collection.rename("ddl_renamed")
    assert db.ddl_renamed.find_one({"_id": 1}) == {"_id": 1}
    db.drop_collection("ddl_renamed")
    assert "ddl_renamed" not in db.list_collection_names()

    # Verify that the restored cluster can also establish new database metadata.
    router.admin.command("enableSharding", "restore_ddl", primaryShard="rs2")
    router.admin.command("shardCollection", "restore_ddl.items", key={"_id": 1})
    router.restore_ddl.items.insert_one({"_id": 1})
    assert router.restore_ddl.items.find_one({"_id": 1}) == {"_id": 1}
    router.restore_ddl.command(
        "create", "items_view", viewOn="items", pipeline=[{"$match": {"_id": 1}}]
    )
    assert list(router.restore_ddl.items_view.find()) == [{"_id": 1}]
    router.restore_ddl.drop_collection("items_view")
    router.restore_ddl.drop_collection("items")
    router.drop_database("restore_ddl")
    assert "restore_ddl" not in router.list_database_names()


def verify_pitr_ddl(router):
    """Check DDL executed while PBM was slicing oplog, before the target time."""
    db = router.pitr_created
    assert "source" not in db.list_collection_names()
    assert list(db.renamed.find()) == [{"_id": "created_during_pitr", "value": 1}]
    assert db.renamed.index_information()["pitr_value_index"]["key"] == [("value", 1)]
    definitions = {entry["name"]: entry for entry in db.list_collections()}
    assert definitions["renamed"]["options"]["validator"] == {"value": {"$type": "number"}}
    assert definitions["created_view"]["options"]["viewOn"] == "renamed"
    assert definitions["created_view"]["options"]["pipeline"] == [{"$match": {"value": 1}}]
    assert list(db.created_view.find()) == [{"_id": "created_during_pitr", "value": 1}]


def reconnect_after_restore(router, members):
    """Replace connections retained across destination recreation and restore."""
    router.close()
    for nodes in members.values():
        for index, node in enumerate(nodes):
            host, port = node.address
            node.close()
            nodes[index] = pymongo.MongoClient(
                f"mongodb://root:root@{host}:{port}/?authSource=admin",
                directConnection=True,
                read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED,
                serverSelectionTimeoutMS=5000,
                timeoutMS=15000,
            )


def recreate_cluster_for_restore(cluster, members, storage):
    """Replace every database container while keeping the existing FS backup directory.

    Reuse the source shard IDs and hostnames. This covers a fresh destination with
    identical topology; shard-name remapping is outside this scenario.
    Capture the new process IDs so the restore assertion checks that the new
    destination processes are not restarted by restore or the PoC.
    """
    cluster.destroy()
    cluster.create()
    config_server = docker.from_env().containers.get(cluster.pbm_cli)
    storage_config = "/tmp/authoritative-restore-fs.conf"
    result = config_server.exec_run([
        "sh", "-ec",
        'sed "s|path: /backups|path: $1|" /etc/pbm-fs.conf > "$2"',
        "setup-fs", storage, storage_config,
    ], user="root")
    assert result.exit_code == 0, result.output
    cluster.setup_pbm(storage_config)
    grant_pbm_catalog_role(members)
    cluster.make_resync()
    return {
        node.address: node.admin.command("hello")["topologyVersion"]["processId"]
        for nodes in members.values() for node in nodes
    }


@pytest.mark.timeout(600)
def test_authoritative_snapshot(restore_cluster):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members, timeseries=False)
    backup = cluster.make_backup("logical")
    perturb_metadata(router, members, catalog)
    simulate_agent_restore(cluster, router, members, storage, backup, catalog)
    verify_restored(cluster, router, members, process_ids, documents)


@pytest.mark.timeout(600)
def test_authoritative_snapshot_timeseries(restore_cluster):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members, timeseries=True)
    backup = cluster.make_backup("logical")
    supplement_timeseries_backup(cluster, members, storage, backup)
    perturb_metadata(router, members, catalog)
    simulate_agent_restore(cluster, router, members, storage, backup, catalog)
    verify_restored(cluster, router, members, process_ids, documents)
    assert list(router.catalog_restore.measurements.find()) == [{"_id": 1, "time": datetime(2026, 1, 1), "sensor": "a", "value": 10}]


@pytest.mark.timeout(600)
def test_authoritative_pitr(restore_cluster):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members, timeseries=False)
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1 --set pitr.compression=none")
    cluster.wait_pitr_chunk()
    crud_checks(router)
    router.catalog_restore.items.with_options(write_concern=WriteConcern(w="majority")).insert_one({"_id": 3})
    documents.append({"_id": 3})
    documents.sort(key=lambda doc: doc["_id"])
    time.sleep(2)
    target = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    time.sleep(2)
    router.catalog_restore.items.insert_one({"_id": 99})
    cluster.disable_pitr(target)
    perturb_metadata(router, members, catalog)
    simulate_agent_restore(cluster, router, members, storage, backup, catalog, target=target)
    verify_restored(cluster, router, members, process_ids, documents)


@pytest.mark.timeout(600)
def test_authoritative_pitr_ddl(restore_cluster):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members, timeseries=False)
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1 --set pitr.compression=none")
    cluster.wait_pitr_chunk()
    crud_checks(router)
    ddl_checks(router)
    router.admin.command("enableSharding", "pitr_created", primaryShard="rs2")
    router.pitr_created.create_collection("source", validator={"value": {"$type": "number"}})
    router.pitr_created.source.insert_one({"_id": "created_during_pitr", "value": 1})
    router.pitr_created.source.rename("renamed")
    router.pitr_created.renamed.create_index("value", name="pitr_value_index")
    router.pitr_created.command(
        "create", "created_view", viewOn="renamed", pipeline=[{"$match": {"value": 1}}]
    )
    # The pre-replay PoC uses the original backup-time catalog. New DDL metadata
    # must be created by replay; injecting a target-time catalog would hide that gap.
    router.catalog_restore.items.with_options(write_concern=WriteConcern(w="majority")).insert_one({"_id": 3})
    documents.append({"_id": 3})
    documents.sort(key=lambda doc: doc["_id"])
    time.sleep(2)
    target = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    time.sleep(2)
    router.catalog_restore.items.insert_one({"_id": 99})
    router.pitr_created.renamed.update_one(
        {"_id": "created_during_pitr"}, {"$set": {"value": 99}}
    )
    cluster.disable_pitr(target)
    perturb_metadata(router, members, catalog)
    simulate_agent_restore(cluster, router, members, storage, backup, catalog, target=target)
    verify_restored(cluster, router, members, process_ids, documents)
    verify_pitr_ddl(router)
    verify_restored(cluster, router, members, process_ids, documents)


@pytest.mark.timeout(600)
def test_authoritative_snapshot_new_cluster(restore_cluster, request):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members, timeseries=False)
    backup = cluster.make_backup("logical")
    process_ids = recreate_cluster_for_restore(cluster, members, storage)
    router.admin.command("balancerStop")
    reconnect_after_restore(router, members)
    router = pymongo.MongoClient(
        cluster.connection, serverSelectionTimeoutMS=5000, timeoutMS=15000
    )
    request.addfinalizer(router.close)
    simulate_agent_restore(cluster, router, members, storage, backup, catalog)
    verify_restored(cluster, router, members, process_ids, documents)


@pytest.mark.timeout(600)
def test_authoritative_snapshot_timeseries_new_cluster(restore_cluster, request):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members, timeseries=True)
    backup = cluster.make_backup("logical")
    supplement_timeseries_backup(cluster, members, storage, backup)
    process_ids = recreate_cluster_for_restore(cluster, members, storage)
    router.admin.command("balancerStop")
    reconnect_after_restore(router, members)
    router = pymongo.MongoClient(
        cluster.connection, serverSelectionTimeoutMS=5000, timeoutMS=15000
    )
    request.addfinalizer(router.close)
    simulate_agent_restore(cluster, router, members, storage, backup, catalog)
    verify_restored(cluster, router, members, process_ids, documents)
    assert list(router.catalog_restore.measurements.find()) == [{"_id": 1, "time": datetime(2026, 1, 1), "sensor": "a", "value": 10}]


@pytest.mark.timeout(600)
def test_authoritative_pitr_new_cluster(restore_cluster, request):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members, timeseries=False)
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1 --set pitr.compression=none")
    cluster.wait_pitr_chunk()
    crud_checks(router)
    router.catalog_restore.items.with_options(write_concern=WriteConcern(w="majority")).insert_one({"_id": 3})
    documents.append({"_id": 3})
    documents.sort(key=lambda doc: doc["_id"])
    time.sleep(2)
    target = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    time.sleep(2)
    router.catalog_restore.items.insert_one({"_id": 99})
    cluster.disable_pitr(target)
    process_ids = recreate_cluster_for_restore(cluster, members, storage)
    router.admin.command("balancerStop")
    reconnect_after_restore(router, members)
    router = pymongo.MongoClient(
        cluster.connection, serverSelectionTimeoutMS=5000, timeoutMS=15000
    )
    request.addfinalizer(router.close)
    simulate_agent_restore(cluster, router, members, storage, backup, catalog, target=target)
    verify_restored(cluster, router, members, process_ids, documents)


@pytest.mark.timeout(600)
def test_authoritative_pitr_ddl_new_cluster(restore_cluster, request):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members, timeseries=False)
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1 --set pitr.compression=none")
    cluster.wait_pitr_chunk()
    crud_checks(router)
    ddl_checks(router)
    router.admin.command("enableSharding", "pitr_created", primaryShard="rs2")
    router.pitr_created.create_collection("source", validator={"value": {"$type": "number"}})
    router.pitr_created.source.insert_one({"_id": "created_during_pitr", "value": 1})
    router.pitr_created.source.rename("renamed")
    router.pitr_created.renamed.create_index("value", name="pitr_value_index")
    router.pitr_created.command(
        "create", "created_view", viewOn="renamed", pipeline=[{"$match": {"value": 1}}]
    )
    # The pre-replay PoC uses the original backup-time catalog. New DDL metadata
    # must be created by replay; injecting a target-time catalog would hide that gap.
    router.catalog_restore.items.with_options(write_concern=WriteConcern(w="majority")).insert_one({"_id": 3})
    documents.append({"_id": 3})
    documents.sort(key=lambda doc: doc["_id"])
    time.sleep(2)
    target = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    time.sleep(2)
    router.catalog_restore.items.insert_one({"_id": 99})
    router.pitr_created.renamed.update_one(
        {"_id": "created_during_pitr"}, {"$set": {"value": 99}}
    )
    cluster.disable_pitr(target)
    process_ids = recreate_cluster_for_restore(cluster, members, storage)
    router.admin.command("balancerStop")
    reconnect_after_restore(router, members)
    router = pymongo.MongoClient(
        cluster.connection, serverSelectionTimeoutMS=5000, timeoutMS=15000
    )
    request.addfinalizer(router.close)
    simulate_agent_restore(cluster, router, members, storage, backup, catalog, target=target)
    verify_restored(cluster, router, members, process_ids, documents)
    verify_pitr_ddl(router)
    verify_restored(cluster, router, members, process_ids, documents)



def partial_catalog_snapshot(members, saved_catalog, namespaces):
    """Replace only selected collection/chunk metadata in the current catalog.

    A partial restore preserves current database versions and unrelated namespaces.
    Rewinding every catalog document to backup time would corrupt metadata for data
    that PBM intentionally leaves at its current state.
    """
    result = {}
    for shard_id, nodes in members.items():
        primary = next(node for node in nodes if node.admin.command("hello")["isWritablePrimary"])
        current = {
            name: list(primary.config[f"shard.catalog.{name}"].find())
            for name in ("databases", "collections", "chunks")
        }
        selected_old = [
            doc for doc in saved_catalog[shard_id]["collections"] if doc["_id"] in namespaces
        ]
        selected_current = [
            doc for doc in current["collections"] if doc["_id"] in namespaces
        ]
        selected_uuids = {doc["uuid"] for doc in selected_old + selected_current}
        current["collections"] = [
            doc for doc in current["collections"] if doc["_id"] not in namespaces
        ] + selected_old
        current["chunks"] = [
            doc for doc in current["chunks"] if doc["uuid"] not in selected_uuids
        ] + [
            doc for doc in saved_catalog[shard_id]["chunks"] if doc["uuid"] in selected_uuids
        ]
        result[shard_id] = current
    return result


def verify_partial_restore(router, members, process_ids, documents):
    assert list(router.catalog_restore.items.find().sort("_id")) == documents
    assert list(router.restore_rs2.ordinary.find().sort("_id")) == [
        {"_id": "first", "value": 1}, {"_id": "second", "value": 2}
    ]
    # Namespace filtering must preserve post-backup writes outside the filter.
    assert router.catalog_restore.ordinary.find_one({"_id": "unselected"}) == {
        "_id": "unselected", "value": 99
    }
    assert router.restore_version.items.find_one({"_id": "unselected"}) == {
        "_id": "unselected"
    }
    assert router.catalog_restore.post_backup.find_one({"_id": 1}) == {"_id": 1}
    assert router.catalog_restore.post_backup.index_information()["marker_index"]["key"] == [("marker", 1)]
    assert list(router.catalog_restore.positive_items.find().sort("_id")) == [
        doc for doc in documents if doc["_id"] > 0
    ]
    crud_checks(router)
    for nodes in members.values():
        for node in nodes:
            assert node.admin.command("hello")["topologyVersion"]["processId"] == process_ids[node.address]


@pytest.mark.timeout(600)
def test_authoritative_snapshot_namespace_filter(restore_cluster):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members)
    backup = cluster.make_backup("logical")
    router.catalog_restore.items.insert_one({"_id": 99})
    router.restore_rs2.ordinary.update_many({}, {"$set": {"value": 99}})
    router.catalog_restore.ordinary.insert_one({"_id": "unselected", "value": 99})
    router.restore_version.items.insert_one({"_id": "unselected"})
    router.catalog_restore.post_backup.insert_one({"_id": 1})
    router.catalog_restore.post_backup.create_index("marker", name="marker_index")
    namespaces = ("catalog_restore.items", "restore_rs2.ordinary")
    expected_catalog = partial_catalog_snapshot(members, catalog, namespaces)
    simulate_agent_restore(cluster, router, members, storage, backup, expected_catalog, namespaces=namespaces)
    verify_partial_restore(router, members, process_ids, documents)


@pytest.mark.timeout(600)
def test_authoritative_pitr_namespace_filter(restore_cluster):
    cluster, router, members, process_ids, storage = restore_cluster
    catalog, documents = seed_data(router, members)
    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1 --set pitr.compression=none")
    cluster.wait_pitr_chunk()
    crud_checks(router)
    router.catalog_restore.items.with_options(write_concern=WriteConcern(w="majority")).insert_one({"_id": 3})
    documents.append({"_id": 3})
    documents.sort(key=lambda doc: doc["_id"])
    time.sleep(2)
    target = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    time.sleep(2)
    router.catalog_restore.items.insert_one({"_id": 99})
    router.restore_rs2.ordinary.update_many({}, {"$set": {"value": 99}})
    router.catalog_restore.ordinary.insert_one({"_id": "unselected", "value": 99})
    router.restore_version.items.insert_one({"_id": "unselected"})
    router.catalog_restore.post_backup.insert_one({"_id": 1})
    router.catalog_restore.post_backup.create_index("marker", name="marker_index")
    cluster.disable_pitr(target)
    namespaces = ("catalog_restore.items", "restore_rs2.ordinary")
    expected_catalog = partial_catalog_snapshot(members, catalog, namespaces)
    simulate_agent_restore(cluster, router, members, storage, backup, expected_catalog, target=target, namespaces=namespaces)
    verify_partial_restore(router, members, process_ids, documents)

