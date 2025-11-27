# Docker Benchmarks for PSMDB-1868

## Start necessary psmdb docker image

```bash
IMAGE=public.ecr.aws/e7j3v3n0/psmdb-build:psmdb-1868 docker compose -f docker-compose-psmdb-1868.yaml up -d psmdb
```

## Run ycsb load

```bash
docker compose -f docker-compose-psmdb-1868.yaml run ycsb
```

## Cleanup everything

```bash
docker compose -f docker-compose-psmdb-1868.yaml down -v --remove-orphans
```

## Repeat with different image and/or modify command options for ycsb

