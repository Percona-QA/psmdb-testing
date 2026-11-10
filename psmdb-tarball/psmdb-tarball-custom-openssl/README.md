# PSMDB OS smoke tests

Checks that `mongod` and `mongos` from the OpenSSL 3 tarball start on a given OS in FIPS mode and serve TLS. One run tests one OS. A CI matrix passes a different `OS_IMAGE` to each job. The binaries need glibc 2.28 or newer.

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

`OS_IMAGE` is the base image. It defaults to `oraclelinux:9`. `PSMDB_VERSION` defaults to `8.0.34-15` and `OPENSSL_VERSION` defaults to `3.5.9`. `test_version` checks that `mongod` and `mongos` report those values.

```bash
OS_IMAGE=ubuntu:22.04 docker compose run --rm --build test
```

Pytest runs with `-vv` (`pytest.ini`). Pass a node id to run one test:

```bash
OS_IMAGE=oraclelinux:9 docker compose run --rm test pytest test_os.py::test_vault -q --tb=short
```

Pass `--client-log`, or set `CLIENT_LOG=1`, to print each client URI and server response.

Compose starts EasyRSA, Cosmian, OpenBao, Kerberos, LDAP, and Keycloak on the `psmdb-os` network. Tests reach them by those hostnames. The test build unpacks the tarballs, runs `fipsinstall`, and starts `mongod` and `mongos` with `LD_LIBRARY_PATH`, `OPENSSL_CONF`, and `OPENSSL_MODULES` set only for those processes.

## Connections

Every `mongod` and `mongos` starts with `--tlsFIPSMode`, `--tlsMode requireTLS`, the EasyRSA CA, a server certificate for that container's hostname, and the `pbm` client certificate as `--tlsClusterFile`. `mongod` also sets `--wiredTigerCacheSizeGB 0.25`. The authentication mechanism is SCRAM-SHA-256 unless the test sets its own list.

The test client connects to the container hostname, verifies that name, and presents a client certificate issued for the `test` hostname. The Kerberos client presents a certificate issued for `kerberos`.

## Tests

| Test | What it checks |
| --- | --- |
| `test_version` | `mongod` and `mongos` report `PSMDB_VERSION` and `OPENSSL_VERSION` |
| `test_standalone_fips_scram` | The log contains `FIPS 140 mode activated`, then a SCRAM-SHA-256 user can authenticate |
| `test_replset_keyfile` | A one-member replica set with a keyfile elects a primary |
| `test_mongos` | A config server replica set accepts a `mongos` connection |
| `test_x509` | `MONGODB-X509` authenticates the `pbm` client certificate |
| `test_ldap` | LDAPS on `ldap:1636` authenticates `cn=pbm,ou=users,dc=example,dc=org` with PLAIN, and libldap verifies the LDAP certificate |
| `test_oidc` | A Keycloak client-credentials token authenticates `keycloak/pbmclient` with `MONGODB-OIDC` |
| `test_kerberos` | GSSAPI authenticates `pbm@PERCONATEST.COM` against the Kerberos KDC |
| `test_kmip` | Cosmian KMIP encrypts a document, `--kmipRotateMasterKey` exits after rotation, and the document is readable after restart |
| `test_vault` | OpenBao encrypts a document, `--vaultRotateMasterKey` writes a new KV version and exits, and the document is readable after restart |
| `test_non_fips_encryption_rejected` | The FIPS OpenSSL configuration encrypts with AES-256-CBC and rejects DES-CBC, RC4, ChaCha20, and Blowfish. `mongod` accepts only `AES256-CBC` and `AES256-GCM` |

## Services

EasyRSA holds the CA, the `pbm` client certificate, and the certificates for Cosmian, Keycloak, LDAP, and Vault. Each test issues a new server certificate whose SAN is the node container name, plus client certificates for `test` and `kerberos`.

| Service | Address | Used by |
| --- | --- | --- |
| Cosmian | `cosmian:5696` | KMIP, TLS client certificate `/etc/pykmip/mongod.pem` |
| OpenBao | `https://vault:8200` | Vault encryption, KV v2 at `secret/`, token `vaulttoken` |
| LDAP | `ldap:1636` | LDAPS, user `pbm`, password `pbmpass` |
| Keycloak | `https://keycloak:8443` | Realm `test`, client `pbmclient`, secret `pbm-secret` |
| Kerberos | `kerberos:88` | Realm `PERCONATEST.COM`, principal `pbm` |
