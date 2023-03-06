# PBM functional tests #

## Setup ##
```docker-compose build```

```docker-compose up -d```

Environment variables for  the setup:

1) PSMDB (default **percona/percona-server-mongodb:latest** ) - base image for the tests
2) PBM_BRANCH (defaut **main**) - branch to build PBM from
3) GO_VER (default **latest**) - golang version 

## Tests ##
TBD

## Run backup/restore tests within the same setup  ##
1) ```docker-compose run test pytest -s --disable-warnings test_sharded.py``` - for single sharded cluster

## Run backup/restore tests between different setups ## 
1) ```docker-compose run test pytest -s --disable-warnings test_fresh_sharded.py``` -for two sharded clusters (src and dst) with the same rsnames

## Run backup/restore tests between different setups ## 
1) ```docker-compose run test pytest -s --disable-warnings test_remap_sharded.py``` -for two sharded clusters (src and dst) with the different rsnames - to check replset-remapping
