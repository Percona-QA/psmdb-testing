## Setup ##

Environment variables for the setup:
1) **MONGODB_IMAGE** (default `percona/percona-server-mongodb:latest`) - base image for the tests
2) **PCSM_BRANCH** (default `main`) - branch, tag, or commit hash to build PCSM from
3) **GO_VER** (default `latest`) - golang version

```bash
docker-compose build                          # Build docker images
docker-compose up -d                          # Create test network
docker-compose --profile monitoring up -d     # Create test network + monitoring (Prometheus/Grafana)
```

## Re-build PCSM image from local repo ##

```bash
docker build --build-context repo=../../percona-clustersync-mongodb . -t csync/local -f Dockerfile-clustersync-local
```

## Run Tests ##

```bash
docker-compose run test pytest test_basic_sync_rs.py -v
docker-compose run test pytest -k test_name --jenkins  # Run specific test or with jenkins flag
```

## Testing Framework ##

### Test Fixtures
- `cluster_configs` - defines cluster topology (replicaset, sharded, etc.)
- `src_cluster` / `dst_cluster` - source and destination MongoDB clusters
- `csync` - PCSM container for synchronization
- `start_cluster` - unified fixture for cluster startup and cleanup

### Custom Pytest Markers

**@pytest.mark.mongod_extra_args("args")**
- Add custom mongod command-line arguments
- Example: `@pytest.mark.mongod_extra_args("--setParameter enableTestCommands=1")`
- Applied to both src/dst clusters via fixtures

**@pytest.mark.csync_log_level("level")**
- Set PCSM log level: `debug` (default), `info`, `trace`, `warn`, `error`
- Example: `@pytest.mark.csync_log_level("trace")`

**@pytest.mark.csync_env({"VAR": "value"})**
- Set environment variables for PCSM container
- Example: `@pytest.mark.csync_env({"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "5"})`

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
- `sharded` - 1-shard cluster → 1-shard cluster
- `sharded_3n` - 3-node sharded → 3-node sharded
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

## Cleanup ##

```bash
docker-compose down -v --remove-orphans
docker-compose --profile monitoring down -v --remove-orphans  # If started with monitoring
```
