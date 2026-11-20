# Custom PSMDB tarball tests

Export variables in the shell before `molecule`. They are not set in the scenario files.

Local Vagrant runs set `LOCAL_TESTING=true`, so Vault is skipped. KMIP is skipped with it. Set `LOCAL_TESTING=false` to install Vault and run those tests.

```bash
cd psmdb-tarball/psmdb-tarball-custom
PSMDB_CUSTOM_BASE_URL=<link to the directory with tarballs> \
PSMDB_VERSION=<version from the tarball filename> \
molecule test -s debian-11-vagrant
```

Vagrant needs the `vagrant` driver, VirtualBox, and Vagrant. EC2 scenario names match `psmdb-tarball/psmdb-tarball`, for example `molecule test -s debian-11`. The Docker scenario is not copied.

`PSMDB_CUSTOM_BASE_URL` is the link to the directory that contains the tarballs. `PSMDB_VERSION` is the version string used in the tarball filename. A value without the Percona revision matches `<version>-<revision>` in the binary output. A value that already includes the revision matches that full version. The target CPU selects the tarball: `x86_64` or `aarch64`.

## Optional variables

| Variable | Default | Effect |
| --- | --- | --- |
| `MONGO_TOOLS_VERSION` | `100.20.1` | Tools tarball version. |
| `MONGO_TOOLS_REVISION` | `1` | Tools tarball revision. |
| `LOCAL_TESTING` | `true` on Vagrant, `false` on EC2 | `true` skips Vault. KMIP is skipped with it. |
| `TESTING_BRANCH` | `main` | Branch used to resolve test metadata. |
| `PYTHON_VERSION` | chosen from `PSMDB_VERSION` | Override the Python built for the tests. |
| `SSH_USER`, `SSH_PUBKEY` | unset | Create an extra login user during prepare. |

Run with Vault installed:

```bash
cd psmdb-tarball/psmdb-tarball-custom
PSMDB_CUSTOM_BASE_URL=<link to the directory with tarballs> \
PSMDB_VERSION=<version from the tarball filename> \
LOCAL_TESTING=false \
molecule test -s debian-11-vagrant
```
