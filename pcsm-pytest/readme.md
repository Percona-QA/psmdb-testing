## Setup ##

Environment variables for  the setup:
1) MONGODB_IMAGE (default **percona/percona-server-mongodb:latest** ) - base image for the tests
2) PCSM_BRANCH (defaut **main**) - branch, or tag, or commithash to build clustersync from
3) GO_VER (default **latest**) - golang version

```docker-compose build``` - build docker images for the tests

```docker-compose up -d``` - create *test* network

```docker-compose --profile monitoring up -d ``` - create *test* network and start prometheus/grafana services for PCSM monitoring

## Re-build PCSM image from local repo ##

```docker build --build-context repo=../../percona-clustersync-mongodb . -t csync/local -f Dockerfile-clustersync-local```

## Run Tests ##

```docker-compose run test pytest test_basic_sync_rs.py -v```

## Cleanup ##

```docker-compose down -v --remove-orphans```

```docker-compose --profile monitoring down -v --remove-orphans ``` - if started with monitoring option
