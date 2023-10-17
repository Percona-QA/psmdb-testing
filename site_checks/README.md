# Testing the availability of various packages/tarballs on the main site

## PSMDB (full PSMDB version needed) 

```
docker run --env PSMDB_VERSION=7.0.2-1 --rm -v .:/tmp -w /tmp python bash -c "pip3 install requests pytest setuptools && env && pytest -s --junitxml=junit.xml test_psmdb.py
```

## PDMDB (full PSMDB version needed)

```
docker run --env PSMDB_VERSION=6.0.9-7 --env PBM_VERSION=2.2.1 --rm -v .:/tmp -w /tmp python bash -c "pip3 install requests pytest setuptools && pytest -s --junitxml=junit.xml test_pdmdb.py"
```

## PBM 

```
docker run --env PBM_VERSION=2.3.0 --rm -v .:/tmp -w /tmp python bash -c "pip3 install requests pytest setuptools && pytest -s --junitxml=junit.xml test_pbm.py"
```
