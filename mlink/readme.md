## Setup ##

Expected location for percona-mongolink to build docker image properly:
```
|- psmdb-testing
|  |- mlink
|- percona-mongolink
```

## Build images for tests

```docker-compose build``` - build docker images for the tests

## Re-build PML image from local repo ##

```docker build --build-context repo=../../percona-mongolink . -t mlink/local -f Dockerfile-mlink-local```

Environment variables for  the setup:
1) MONGO_VERSION (default **mongo:6.0** ) - base image for the tests
2) MLINK_BRANCH (defaut **main**) - branch, or tag, or commithash to build mlink from
3) GO_VER (default **latest**) - golang version

```docker-compose up -d``` - create *test* network

```docker-compose --profile monitoring up -d ``` - create *test* network and start prometheus/grafana services for PML monitoring

## Run Tests ##

1) ```docker-compose run test pytest test_basic_sync_rs.py -v```

## Cleanup ##

```docker-compose down -v --remove-orphans```

```docker-compose --profile monitoring down -v --remove-orphans ``` - if started with monitoring option
