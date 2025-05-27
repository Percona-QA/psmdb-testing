## Setup ##

Environment variables for  the setup:
1) MONGO_VERSION (default **percona/percona-server-mongodb:latest** ) - base image for the tests
2) MLINK_BRANCH (defaut **main**) - branch, or tag, or commithash to build mlink from
3) GO_VER (default **latest**) - golang version

```docker-compose build``` - build docker images for the tests

```docker-compose up -d``` - create *test* network

```docker-compose --profile monitoring up -d ``` - create *test* network and start prometheus/grafana services for PML monitoring

## Re-build PML image from local repo ##

```docker build --build-context repo=../../percona-mongolink . -t mlink/local -f Dockerfile-mlink-local```

## Run Tests ##

```docker-compose run test pytest test_basic_sync_rs.py -v```

## Cleanup ##

```docker-compose down -v --remove-orphans```

```docker-compose --profile monitoring down -v --remove-orphans ``` - if started with monitoring option
