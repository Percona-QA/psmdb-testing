## Setup ##

```
!!! Copy mlink repo to local folder
> git clone git@github.com:Percona-Lab/percona-mongolink.git

docker-compose build``` - build docker images for the tests

Environment variables for  the setup:
1) MONGO_VERSION (default **mongo:6.0** ) - base image for the tests
2) MLINK_BRANCH (defaut **main**) - branch, or tag, or commithash to build mlink from
3) GO_VER (default **latest**) - golang version

```docker-compose up -d``` - create *test* network

## Run Tests ##

1) ```docker-compose run test pytest test_sharded.py```

## Cleanup ##

```docker-compose down -v --remove-orphans```
