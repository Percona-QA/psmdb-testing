## Prerequisites ##

Image builds use Compose [`additional_contexts`](https://docs.docker.com/compose/compose-file/build/#additional_contexts) so MongoDB and PCSM images can `FROM easyrsa/local`. That requires **Docker Compose v2.17 or later** (the `docker compose` plugin, with BuildKit). The Python `docker-compose` v1 CLI does not support this option and will fail before tests start.

```bash
docker compose version   # e.g. Docker Compose version v2.29.0
```

## Setup ##

`docker compose build` produces three MongoDB images so single-version and cross-version layouts can coexist:

| Image               | When it is used                                       | Base image (build arg)                             |
| ------------------- | ----------------------------------------------------- | -------------------------------------------------- |
| `mongodb/local`     | Same MongoDB version on source and target             | `MONGODB_IMAGE`                                    |
| `mongodb-src/local` | Source cluster when source and target versions differ | `MONGODB_SRC_IMAGE`, falls back to `MONGODB_IMAGE` |
| `mongodb-dst/local` | Target cluster when source and target versions differ | `MONGODB_DST_IMAGE`, falls back to `MONGODB_IMAGE` |

Environment variables for the setup:
1) **MONGODB_IMAGE** (default `percona/percona-server-mongodb:latest`) - base image used everywhere when no per-side override is set
2) **MONGODB_SRC_IMAGE** (optional, falls back to `MONGODB_IMAGE`) - base image for the source cluster, used to build the `mongodb-src/local` image
3) **MONGODB_DST_IMAGE** (optional, falls back to `MONGODB_IMAGE`) - base image for the target cluster, used to build the `mongodb-dst/local` image
4) **PCSM_BRANCH** (default `main`) - branch, tag, or commit hash to build PCSM from
5) **GO_VER** (default `latest`) - golang version

To run the suite with different MongoDB versions on source and target (cross-version replication), export both `MONGODB_SRC_IMAGE` and `MONGODB_DST_IMAGE` before building:

```bash
export MONGODB_SRC_IMAGE=perconalab/percona-server-mongodb:6.0
export MONGODB_DST_IMAGE=perconalab/percona-server-mongodb:7.0
docker compose build
docker compose up -d
```

```bash
docker compose build                          # Build docker images
docker compose up -d                          # Create test network
docker compose --profile monitoring up -d     # Create test network + monitoring (Prometheus/Grafana)
```

## Re-build PCSM image from local repo ##

```bash
docker build --build-context repo=../../percona-clustersync-mongodb . -t csync/local -f Dockerfile-clustersync-local
```

## Run Tests ##

```bash
docker compose run test pytest test_basic_sync_rs.py -v
docker compose run test pytest -k test_name --jenkins  # Run specific test or with jenkins flag
```

## Testing Framework ##

### Test Fixtures
- `cluster_configs` - defines cluster topology (replicaset, sharded, etc.)
- `src_cluster` / `dst_cluster` - source and destination MongoDB clusters
- `csync` - PCSM container for synchronization
- `start_cluster` - unified fixture for cluster startup and cleanup
- `start_ha_cluster` - same, but starts a group of PCSM instances sharing one
  lease on the target instead of a single `csync`. Yields a `PCSMGroup` (see
  [High Availability tests](#high-availability-tests))

### Custom Pytest Markers

**@pytest.mark.mongod_extra_args("args")**
- Add custom mongod command-line arguments
- Example: `@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")`
- Applied to both src/dst clusters via fixtures

**@pytest.mark.mongos_extra_args("args")**
- Add custom mongos command-line arguments, for sharded topologies
- Example: `@pytest.mark.mongos_extra_args("--setParameter enableTestCommands=1")`
- Applied to the dst cluster

**@pytest.mark.csync_log_level("level")**
- Set PCSM log level: `debug` (default), `info`, `trace`, `warn`, `error`
- Example: `@pytest.mark.csync_log_level("trace")`

**@pytest.mark.csync_env({"VAR": "value"})**
- Set environment variables for PCSM container
- Example: `@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "5"})`

**@pytest.mark.ha_instances(n)**
- Number of PCSM instances `start_ha_cluster` brings up, default 3
- Example: `@pytest.mark.ha_instances(5)`

**@pytest.mark.jenkins**
- Mark tests to run only with `--jenkins` flag (excluded by default)

**@pytest.mark.timeout(seconds, func_only=True)**
- Set test timeout using pytest-timeout plugin
- Example: `@pytest.mark.timeout(300, func_only=True)` - 5 minute timeout
- Common values: 300s (default), 600s (long tests), 1200s (very long tests)
- `func_only=True` applies timeout only to test function, not fixtures

### Cluster Configurations

Available topologies via `@pytest.mark.parametrize("cluster_configs", [...], indirect=True)`:
- `replicaset` - 1-node RS → 1-node RS
- `replicaset_3n` - 3-node RS → 3-node RS
- `sharded` - 2-shard cluster → 2-shard cluster
- `sharded_3n` - 3-node sharded → 3-node sharded
- `sharded_3v2` - 3-shard cluster → 2-shard cluster (unequal shard counts)
- `sharded_2v3` - 2-shard cluster → 3-shard cluster (unequal shard counts)
- `rs_sharded` - RS → sharded cluster
- `sharded_rs` - sharded cluster → RS

### Example Test

```python
@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.csync_log_level("trace")
@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "10"})
def test_example(start_cluster, src_cluster, dst_cluster, csync):
    # Your test code here
    pass
```

### High Availability tests

PCSM HA is always on: instances contend for one lease on the target, exactly
one becomes ACTIVE and drives replication, the rest stay STANDBY and reject
operational endpoints with HTTP 409 `not_active`. `start_ha_cluster` brings up
a group of them against the same source and target, so no extra setup is
needed beyond the normal `docker compose build`.

```bash
docker compose run test pytest test_ha.py -v
docker compose run test pytest test_ha.py -k "T127 and sharded" -v
```

The fixture yields a `PCSMGroup`. Instances are named `csync0`, `csync1`, ...
and share `--group-name=qa`; each one is an ordinary `Clustersync`, so the
usual `start()`, `finalize()` and `logs()` calls work on it.

| Call | Purpose |
| --- | --- |
| `group.active()` | The ACTIVE instance, waiting up to 30s for one to settle |
| `group.standbys()` | Live instances that are not ACTIVE |
| `group.alive_instances()` | Instances whose container is still running |
| `group.wait_for_single_active()` | Block until exactly one ACTIVE is seen |
| `group.kill_active()` | SIGKILL the ACTIVE and return it, simulating a crash |
| `group.add_instance(reset=False)` | Add a member to a running group |
| `group.logs()` | Logs from every instance |

Faults are injected through the instance itself: `kill()`, `start_container()`,
`pause_container()`, `disconnect_network()` and `connect_network()`. A killed
instance leaves its member document behind and is dropped from the group
roster by heartbeat age, so check `group.active().status()` rather than the
`members` collection when asserting membership.

```python
@pytest.mark.ha_instances(5)
@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_example_ha(start_ha_cluster, src_cluster, dst_cluster):
    group = start_ha_cluster
    active = group.active()
    assert active.start(), "Failed to start csync on ACTIVE"
    assert active.wait_for_repl_stage(), "Failed to reach replication stage"
    killed = group.kill_active()
    group.wait_for_single_active()
    promoted = group.active()
    assert promoted.name != killed.name
```

## Cleanup ##

```bash
docker compose down -v --remove-orphans
docker compose --profile monitoring down -v --remove-orphans  # If started with monitoring
```
