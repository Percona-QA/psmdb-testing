# PBM functional tests #

## Setup ##
```docker-compose build```

```docker-compose up -d```

Environment variables for  the setup:

1) PSMDB (default **percona/percona-server-mongodb:latest** ) - base image for the tests
2) PBM_BRANCH (defaut **main**) - branch to build PBM from
3) GO_VER (default **latest**) - golang version 

## Tests ##
The function_scoped_container_getter fixture uses "function" scope, meaning that all of the containers are torn down after each individual test.

## Run backup/restore tests within the same setup  ##
1) ```docker-compose run test pytest --docker-compose=docker-compose-rs.yaml -s --disable-warnings test_rs.py``` - for replicaset 
2) ```docker-compose run test pytest --docker-compose=docker-compose-sharded.yaml -s --disable-warnings test_sharded.py``` - for sharded cluster

## Run backup/restore tests between different setups ## 
1) ```docker-compose run test pytest --docker-compose=docker-compose-rs.yaml,docker-compose-fresh-rs.yaml -s --disable-warnings test_fresh_rs.py``` - for replicaset
2) ```docker-compose run test pytest --docker-compose=docker-compose-sharded.yaml,docker-compose-fresh-sharded.yaml -s --disable-warnings test_fresh_sharded.py``` -for sharded
