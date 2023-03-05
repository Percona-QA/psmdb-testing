# PBM functional tests #

## Setup ##
```docker-compose build```

```docker-compose up -d```

Environment variables for  the setup:

1) PSMDB (default **percona/percona-server-mongodb:latest** ) - base image for the tests
2) PBM_BRANCH (defaut **main**) - branch to build PBM from
3) GO_VER (default **latest**) - golang version 

## Tests ##
Each test contains fixture with function scope for creation necessary environment using ```create_replicaset(rs)``` or ```create_sharded(mongos,cluster)``` functions, example:

```create_sharded("mongos",[{ "rscfg": [ "rscfg01", "rscfg02", "rscfg03" ]},{ "rs1": [ "rs101", "rs102", "rs103" ]},{ "rs2": [ "rs201", "rs202", "rs203" ]}])```

Be sure to "label" the rsname of the config server with "cfg" name


## Run backup/restore tests within the same setup  ##
1) ```docker-compose run test pytest -s --disable-warnings test_sharded.py``` - for single sharded cluster

## Run backup/restore tests between different setups ## 
1) ```docker-compose run test pytest -s --disable-warnings test_fresh_sharded.py``` -for two sharded clusters (src and dst) with the same rsnames

## Run backup/restore tests between different setups ## 
1) ```docker-compose run test pytest -s --disable-warnings test_remap_sharded.py``` -for two sharded clusters (src and dst) with the different rsnames - to check replset-remapping
