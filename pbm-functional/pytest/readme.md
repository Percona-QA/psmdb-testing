# PMM functional tests #

## Setup ##
```docker-compose build```

```docker-compose up -d```

## Tests ##
The function_scoped_container_getter fixture uses "function" scope, meaning that all of the containers are torn down after each individual test.

## Run backup/restore tests within the same setup  ##
```docker-compose run test pytest --docker-compose=docker-compose-rs.yaml -s --verbose --disable-warnings test_rs.py```
## Run backup/restore tests between different setups ## 
```docker-compose run test pytest --docker-compose=docker-compose-rs.yaml,docker-compose-fresh-rs.yaml -s --verbose --disable-warnings test_fresh_rs.py```
