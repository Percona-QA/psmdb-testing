# Custom PSMDB package tests

Export variables in the shell before `molecule`. They are not set in the scenario files.

Local Vagrant runs set `LOCAL_TESTING=true`, so Vault is skipped, along with KMIP, OIDC, and AWS IAM. Set `LOCAL_TESTING=false` to install Vault and run those tests.

```bash
cd psmdb/psmdb-custom
PSMDB_CUSTOM_BASE_URL=<link to the directory with packages> \
PSMDB_VERSION=<version from the package filename> \
molecule test -s debian-11-vagrant
```

`centos-7-vagrant` uses the same variables. `el9.8-fips-vagrant` and `el9.6-fips-vagrant` use the Rocky Linux 9.8 and 9.6 VirtualBox boxes. Both enable FIPS mode with `fips-mode-setup --enable`, then reboot and check that FIPS mode is enabled. OpenSSL stays at the version shipped in the box. EC2 scenario names match `psmdb/psmdb`, for example `molecule test -s debian-11`. Vagrant needs the `vagrant` driver, VirtualBox, and Vagrant.

`PSMDB_CUSTOM_BASE_URL` is the link to the directory that contains the packages. `PSMDB_VERSION` is the version string used in the package filename. A value without the Percona revision matches `<version>-<revision>` in the binary output. A value that already includes the revision matches that full version. The host CPU selects the package: `amd64` for x86_64, `arm64` for aarch64.

Set `OIDC_CLIENT_ID` and `OIDC_CLIENT_SECRET` when OIDC is enabled (`LOCAL_TESTING=false`, which is the EC2 default).

## Optional variables

| Variable | Default | Effect |
| --- | --- | --- |
| `PSMDB_MONGOSH_VERSION` | `2.14.1` | Mongosh package version. |
| `MONGO_TOOLS_VERSION` | `100.20.1` | Tools version. If that package is missing, the server version is used. |
| `MONGO_TOOLS_REVISION` | `1` | Tools package revision. |
| `PSMDB_INSTALL_DBG` | `true` | Install debug packages. Set `false` to skip them. |
| `LOCAL_TESTING` | `true` on Vagrant, `false` on EC2 | `true` skips Vault. KMIP, OIDC, and AWS IAM are skipped with it. |
| `ENABLE_TOOLKIT` | `false` | Install Percona Toolkit. |
| `JOB_NAME` | scenario name, or `local` | A name containing `fips` turns on FIPS mongod settings. `el9.8-fips-vagrant` and `el9.6-fips-vagrant` do this when `JOB_NAME` is unset. |
| `TESTING_BRANCH` | `main` | Branch shared tests use to load mongosh and tools versions. |

Full local run:

```bash
cd psmdb/psmdb-custom
PSMDB_CUSTOM_BASE_URL=<link to the directory with packages> \
PSMDB_VERSION=<version from the package filename> \
LOCAL_TESTING=false \
OIDC_CLIENT_ID=... \
OIDC_CLIENT_SECRET=... \
molecule test -s debian-11-vagrant
```
