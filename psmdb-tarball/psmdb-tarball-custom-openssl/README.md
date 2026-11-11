# PSMDB OS smoke tests

Checks that `mongod` and `mongos` from the OpenSSL 3 tarball start on a given OS in FIPS mode and serve TLS. One run tests one OS. A CI matrix passes a different `OS_IMAGE` to each job. The binaries need glibc 2.28 or newer.

## Tarballs

Download both archives into this directory before building:

- `percona-server-mongodb-*.tar.gz` — server binaries
- `percona-server-mongodb-*-runtime-libs.tar.gz` — OpenSSL, FIPS provider, Kerberos, and SASL libraries

The image build fails if either file is missing. The runtime-libs name must contain `-runtime-libs`.

GitHub Actions (`.github/workflows/PSMDB-CUSTOM-OPENSSL3.yml`) downloads the server tarball from the repository secret `PSMDB_CUSTOM_TARBALL_URL`. The final path segment, without the query string, is the filename saved into this directory. The workflow keeps that query string and builds the other three URLs by replacing the `.tar.gz` suffix:

- `<name>-runtime-libs.tar.gz`
- `<name>.sbom.cdx.json`
- `<name>-runtime-libs.sbom.cdx.json`

The workflow runs on `ubuntu-22.04`, once for each OS image in its matrix, and publishes the JUnit report written to `junit.xml`.

## Run

From this directory:

```bash
OS_IMAGE=oraclelinux:9 docker compose run --rm --build test
```

`OS_IMAGE` is the base image. It defaults to `oraclelinux:9`. `PSMDB_VERSION` defaults to `8.0.34-15` and `OPENSSL_VERSION` defaults to `3.5.9`. `test_version` checks that `mongod` and `mongos` report those values, and that `ldd` resolves Kerberos and SASL from the bundle's `opt/mongo/openssl3/lib` and OpenLDAP, curl, OpenSSL, zlib, and c-ares from the bundle's `lib`.

```bash
OS_IMAGE=ubuntu:22.04 docker compose run --rm --build test
```

Pytest runs with `-vv` (`pytest.ini`). Pass a node id to run one test:

```bash
OS_IMAGE=oraclelinux:9 docker compose run --rm test pytest test_os.py::test_vault -q --tb=short
```

Pass `--client-log`, or set `CLIENT_LOG=1`, to print each client URI and server response.

Compose starts EasyRSA, Cosmian, OpenBao, Kerberos, LDAP, and Keycloak on the `psmdb-os` network and waits until their health checks pass. Tests reach them by those hostnames. The test build unpacks the tarballs, runs `fipsinstall`, and starts `mongod` and `mongos` with `LD_LIBRARY_PATH`, `OPENSSL_CONF`, and `OPENSSL_MODULES` set only for those processes.

## Connections

Every `mongod` and `mongos` starts with `--tlsFIPSMode`, `--tlsMode requireTLS`, the EasyRSA CA, a server certificate for that container's hostname, and the `pbm` client certificate as `--tlsClusterFile`. `mongod` also sets `--wiredTigerCacheSizeGB 0.25`. The authentication mechanism is SCRAM-SHA-256 unless the test sets its own list.

The test client connects to the container hostname, verifies that name, and presents a client certificate issued for the `test` hostname. The Kerberos client presents a certificate issued for `kerberos`.

## Tests

| Test | What it checks |
| --- | --- |
| `test_version` | `mongod` and `mongos` report `PSMDB_VERSION` and `OPENSSL_VERSION`, and the bundled Kerberos, SASL, LDAP, curl, OpenSSL, zlib, and c-ares libraries are the ones `mongod` loads |
| `test_server_sbom` | The server CycloneDX SBOM names this PSMDB version, records the tarball SHA-256, lists OpenSSL `OPENSSL_VERSION`, and its commit matches `mongod --version` |
| `test_runtime_libs_sbom` | The runtime-libs CycloneDX SBOM records that tarball's SHA-256 and lists bundled OpenSSL, zlib, c-ares, krb5, Cyrus SASL, OpenLDAP, and curl |
| `test_sbom_vulnerability_report` | Grype scans both SBOMs for fixed vulnerabilities and writes a table report beside each SBOM. Findings are reported; a grype failure fails the test |
| `test_standalone_fips_scram` | The log contains `FIPS 140 mode activated`, then a SCRAM-SHA-256 user can authenticate |
| `test_replset_keyfile` | A one-member replica set with a keyfile elects a primary |
| `test_mongos` | A config server replica set accepts a `mongos` connection |
| `test_x509` | `MONGODB-X509` authenticates the `pbm` client certificate |
| `test_ldap` | LDAPS on `ldap:1636` authenticates `cn=pbm,ou=users,dc=example,dc=org` with PLAIN. The `cn=readers,ou=groups,dc=example,dc=org` group grants `readWriteAnyDatabase`, and the test writes a document with that role |
| `test_oidc` | A Keycloak client-credentials token carries a `client_id` claim of `pbmclient` and authenticates `keycloak/pbmclient` with `MONGODB-OIDC` |
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
