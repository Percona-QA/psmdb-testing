# PBM e2e-tests #

## Setup ##

```docker-compose build``` - build docker images for the tests, locally consider using it with *--no-cache* to avoid caching while rebuilding images with new versions.

Environment variables for  the setup:

1) PSMDB (default **percona/percona-server-mongodb:latest** ) - base image for the tests
2) PBM_BRANCH (defaut **main**) - branch, or tag, or commithash to build PBM from
3) GO_VER (default **latest**) - golang version 

```docker-compose up -d``` - create *test* network, start and configure storages for tests  - volume *fs* as fs-storage, container *minio* as AWS S3-like storage and container *azurite* as emulator for Azure Blob Storage.

## Re-build image with local repo ##

```docker build --build-context repo=../../../percona-backup-mongodb . -t replica_member/local -f Dockerfile-local```


## Run Tests ##

1) ```docker-compose run test pytest``` - run all tests 

2) ```docker-compose run test pytest test_sharded.py``` - run all tests from the file *test_sharded.py* 

3) ```docker-compose run test pytest -k logical``` - run all tests containing *logical* in the test name from all files, see https://docs.pytest.org/en/7.1.x/how-to/usage.html#specifying-which-tests-to-run for more details

4) ```docker-compose run test pytest -k logical test_sharded.py``` - run tests containing *logical* in the test name from the file *test_sharded.py*

5) ```docker-compose run test pytest -s``` - with output to stdout

6) ```docker-compose run test pytest -s --verbose``` - more verbose output, also get logs from all containers after each test ends

## Cleanup ##

```docker-compose down -v --remove-orphans```


#
# Writing new tests #

Consider using the following template:
```
import pytest

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return *your_cluster_config*

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        *some_extra_preconfiguration*
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_first(start_cluster,cluster):  
    *your test*    

@pytest.mark.timeout(600,func_only=True)
def test_second(start_cluster,cluster):  
    *your test*  
```

Where *cluster_config* is json that describe your cluster configuration, could be on of the following:

 - ```{ _id: "rsname", members: [{host: "rshost1"}, {host: "rshost2", hidden: boolean, priority: int, arbiterOnly: bool}, ...]} ``` for single replicaset setup

 - ```{ mongos: 'host', configserver: { replicaset }, shards: [replicaset1, replicaset2 ...]}``` for sharded cluster

Some of the class **Cluster** methods:

- **cluster.create()** - creates docker containers, attach them to test network, setups replicaset(s), setup sharded-cluster, setups authorization

- **cluster.destroy()** - destroys all containers

- **cluster.setup_pbm()** - configure pbm with minio storage, the same as ```pbm config --file=/etc/pbm.conf --out=json```

- **cluster.make_backup(type)** - creates backup, returns the **name** of the backup, the same as ```pbm backup --type=type``` 

- **cluster.make_restore(name)** - restore cluster from the backup **name**, also accepts extra options:

    *restart_cluster=bool* - whether or not restart the whole cluster after the restore is complete, useful for restoring from physical or incremental backups
   
    *make_resync=bool* - whether or not perform ```pbm config --force-resync``` after the restore is complete
   
    *check_pbm_status=bool* - if true check ```pbm status``` , raises an AssertionError if not all pbm agents have the status "Ok"

- **cluster.exec_pbm_cli(command)** - runs any pbm command e.g. ```cluster.exec_pbm_cli("status")``` the same as ```pbm status```, returns rc (exit status), stdout and stderr
