# Possible PBM logical restore finalization for authoritative shards

PSMDB 9.0 logical restore can leave a sharded cluster unable to serve requests after data restoration completes. PBM currently omits the shard-local authoritative catalogs. Direct restoration of those documents alone does not rebuild the in-memory database metadata. This PoC demonstrates a possible PBM implementation using existing server commands, without server code changes or mongod restarts.

The tested PBM 2.17.0 could complete simpler logical restores, but DDL recorded during PITR generated authoritative metadata oplog events such as invalidateCollectionMetadata. PBM rejected these as unknown commands and stopped before the proposed finalization could run. Time-series restoration also failed during collection creation, and the inspected backup omitted the viewless time-series data. These blockers prevented exercising the authoritative-shard PoC through the existing restore command.

The simulation retains PBM backups and FS storage, but replaces agent-driven restore with the installed mongorestore 100.19.1. It reproduces internal database/collection cleanup and destination topology exclusions. During replay, authoritative metadata events are filtered, including inside transaction envelopes, and their effect is deferred to catalog reconstruction and finalization. Transaction buffering and commit/abort processing are delegated to mongorestore. The adapter also handles the recreated system.views UUID, filters operational config collections, and strips physical create metadata from o2, following the corresponding PBM behavior. These steps allow the finalization to be exercised after DDL replay; they do not establish complete equivalence with the agent's distributed replay.

The tested PBM also omits restoration of config.shard.catalog.databases, config.shard.catalog.collections and config.shard.catalog.chunks on individual shards. The simulation explicitly reconstructs these local catalogs using saved collection/chunk metadata and database ownership and versions from the global catalog after replay. It then executes the internal metadata commands to finalize the restored state on primaries and secondaries. This is the central functional addition being proposed for PBM; the saved catalog supplied by the tests remains a stand-in for production catalog backup and point-in-time reconstruction.

For time-series controls only, the simulation supplements backup data through rawData and explicitly supplies clusteredIndex during collection creation. These are experimental compatibility adaptations. The proposed PBM change below remains restoration and finalization of authoritative shard metadata.

## Proposed PBM sequence

Add the following finalization after the existing snapshot restore and oplog replay, before reporting restore completion.

1. Restore the shard-local authoritative catalogs on each destination shard's primary. Use database ownership and versions from the global catalog after replay, including databases created during PITR. Collection and chunk metadata must match the restored global catalog and destination shard IDs. Wait for majority acknowledgement.
2. For each restored database owned by that shard, execute `_shardsvrCommitCreateDatabaseMetadata` with its restored database version. Execute it in the database's own context, because `$db` determines the database being committed.
   ```javascript
   // Connect directly to the primary of the shard owning catalog_restore.
   const session = db.getMongo().startSession();
   const metadata = session.getDatabase("config")
       .getCollection("shard.catalog.databases")
       .findOne({_id: "catalog_restore"});

   assert.commandWorked(session.getDatabase("catalog_restore").runCommand({
       _shardsvrCommitCreateDatabaseMetadata: "catalog_restore",
       dbVersion: metadata.version,
       txnNumber: NumberLong(0),
       writeConcern: {w: "majority"}
   }));
   session.endSession();
   ```

3. For each relevant collection on the shard, execute `_shardsvrFetchCollMetadata` with the full namespace and the database's primary shard ID from the restored catalog.
   ```javascript
   // Connect directly to a relevant shard primary.
   // rs1 is catalog_restore's primary shard ID from the restored global catalog.
   const fetchSession = db.getMongo().startSession();
   assert.commandWorked(fetchSession.getDatabase("admin").runCommand({
       _shardsvrFetchCollMetadata: "catalog_restore.items",
       primaryShardId: "rs1",
       txnNumber: NumberLong(0),
       writeConcern: {w: "majority"}
   }));
   fetchSession.endSession();
   ```

4. Supply a session, an explicit `txnNumber` and `writeConcern: {w: "majority"}` for both commands. The server requires a retryable-write context to track command completion across retries and majority acknowledgement to confirm the writes on a majority of the replica set. Wait until every member that will serve requests has applied the final metadata operations, since majority acknowledgement does not establish application on every secondary.
5. Report restore completion only after the catalog writes and metadata finalization have succeeded.

Commands run only on each shard's primary. Secondaries receive catalog writes and metadata events through replication. Config shards must also be handled as shards; this topology has not been tested.

For namespace-filtered restore, preserve current database metadata and all unrelated catalog entries. Replace only selected collections' metadata and corresponding chunks. Rewinding the entire catalog would also rewind metadata for namespaces excluded from restore. The PoC merges saved selected metadata with current unselected metadata.

## Additional PBM privileges

Both commands require the cluster-level `internal` action. The existing root account was rejected with `Unauthorized`. The PoC grants a custom role to PBM on each shard replica set before backup and when creating the destination. Production integration must establish the privilege on every destination shard.

```javascript
{resource: {cluster: true}, actions: ["internal"]}
```

This powerful internal privilege and the production authorization model require review before adopting the calls in PBM.

## Commands and history

`_shardsvrCommitCreateDatabaseMetadata` upserts the local database catalog entry, updates the primary's in-memory database metadata and emits a `createDatabaseMetadata` oplog event that updates secondaries. It was introduced by the database metadata commit changes associated with SERVER-99932, SERVER-101059 and SERVER-101074. Passing the name only as the command argument is insufficient; `$db` must identify the intended database.

[PSMDB commit 9a2fdb1](https://github.com/percona/percona-server-mongodb/commit/9a2fdb149ccbc720eb91e24d93830565344f6ac4) · [SERVER-99932](https://jira.mongodb.org/browse/SERVER-99932) · [SERVER-101059](https://jira.mongodb.org/browse/SERVER-101059) · [SERVER-101074](https://jira.mongodb.org/browse/SERVER-101074)

`_shardsvrFetchCollMetadata` fetches collection metadata and owned chunks from the restored config server and persists them in the shard-local catalog. In the current server checkout, the clone path persists metadata and conditionally invalidates stale in-memory collection metadata so it can be recovered from the local catalog. Removing the call caused all six non-time-series scenarios to fail during CRUD with `No progress was made executing write ops`. Restoring the call made both snapshot control tests pass. Its necessity was demonstrated for those scenarios; the precise causal path in the previously tested binary was not established.

[Introduction in PSMDB](https://github.com/percona/percona-server-mongodb/commit/a478326877d7aecc8f4579e4c6f0dfeb95e2f37c) · [SERVER-101402](https://jira.mongodb.org/browse/SERVER-101402) · [Use by refineCollectionShardKey](https://github.com/percona/percona-server-mongodb/commit/0f188e549f7e359cd5ba7a6b3d887b789e276e2e) · [SERVER-103031](https://jira.mongodb.org/browse/SERVER-103031) · [SERVER-102541](https://jira.mongodb.org/browse/SERVER-102541)

## Verified behavior and PoC boundaries

The PoC uses FS storage, two shards with three data-bearing members each, databases with primary shards rs1 and rs2, a sharded collection with four initial chunks, ordinary collections, indexes, validators and views.

Logical snapshot restore and PITR passed on the existing cluster and on a recreated cluster with identical shard IDs and hostnames, including databases owned by different primary shards.

Namespace-filtered snapshot restore and PITR passed for a sharded collection and an ordinary collection in the other database. Post-backup writes outside the filter and a newly created collection and index were preserved. These tests restore into the existing cluster.

The PoC supplies saved local collection/chunk metadata manually to simulate missing PBM catalog restoration. After replay, it obtains the final database versions and collection namespace list from the restored global catalog. DDL during PITR, including a new database, collection rename, index and view creation, passed on both existing and recreated clusters. Production integration must restore catalog state consistently through the target timestamp and remove obsolete metadata.

The eight non-time-series scenarios passed across the full run and targeted reruns after fixes. The final shared implementation was not rerun as one complete suite after the last changes.

## Time-series and views

Two additional compatibility issues initially blocked time-series restore: PBM omitted viewless time-series data from the backup, and creating the collection through applyOps without an explicit clusteredIndex caused the server to synthesize an incompatible _id index.

Both snapshot scenarios passed after the PoC supplemented raw bucket data using rawData and supplied clusteredIndex explicitly during restore. This supplemental read assumes no concurrent writes and is not a production backup implementation. PBM requires integrated rawData dumping with its normal snapshot/oplog coordination. The installed mongorestore 100.19.1 already supports rawData.

Ordinary views, including changes replayed during PITR, passed. These compatibility adaptations support the experiment; the proposed PBM change remains authoritative-catalog restoration and finalization.

## Remaining validation

- Additional catalog DDL during PITR, particularly database primary changes and recreation of an existing namespace with a different UUID.
- Clearing stale in-memory metadata for objects absent from the restored catalog.
- Primary failover during or after finalization and idempotent retry after partial failure.
- Destination shard remapping, different topologies and config shards.
- Internal-command compatibility and the production privilege model across supported server versions.
