# PSMDB OS smoke tests

Checks that `mongod` and `mongos` from the OpenSSL 3 tarball start on a given OS and can use SCRAM-SHA-256, a keyfile replica set, `mongos`, x509, LDAP, OIDC, KMIP, and Vault.

One run tests one OS. A CI matrix passes a different `OS_IMAGE` to each job. The binaries need glibc 2.28 or newer.

## Tarballs

Download both archives into this directory before building:

- `percona-server-mongodb-*.tar.gz` — server binaries
- `percona-server-mongodb-*-runtime-libs.tar.gz` — OpenSSL, FIPS provider, Kerberos, and SASL libraries

The image build fails if either file is missing. The runtime-libs name must contain `-runtime-libs`.

## Run

From this directory:

```bash
OS_IMAGE=oraclelinux:9 docker compose run --rm --build test
```

`OS_IMAGE` is the base image. It defaults to `oraclelinux:9`.

```bash
OS_IMAGE=ubuntu:22.04 docker compose run --rm --build test
```

Compose starts EasyRSA, Cosmian, Vault, Kerberos, LDAP, and Keycloak on the `psmdb-os` network. The test build unpacks the tarballs, runs `fipsinstall`, and starts `mongod` and `mongos` with `LD_LIBRARY_PATH`, `OPENSSL_CONF`, and `OPENSSL_MODULES` set only for those processes. Pytest runs with `-vv` (`pytest.ini`).
