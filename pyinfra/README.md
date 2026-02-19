# Pyinfra runner for package tests

This directory contains a **pytest-centric** test runner that uses **pyinfra** (API, no CLI), **boto3** or **Vagrant** for provisioning, and **testinfra** for assertions.

**Run everything from this directory** (`pyinfra`). Do not run pytest from the repo root.

## How it works

1. **Fixture `prepare_instance`** (in the test module, e.g. `test_pbm.py`): creates an instance (EC2 or Vagrant), runs **prepare** and **setup_pbm** via the pyinfra Python API, yields the paramiko connection string for testinfra. On teardown it destroys the instance unless you passed `--keep-instance`.
2. **Fixture `host`**: uses `prepare_instance` and returns `testinfra.get_host(connection)` so tests receive a testinfra host.
3. **Tests** use the `host` fixture; no `INSTANCE_CONFIG` or `testinfra_hosts` in the environment.

### Running

```bash
# Full run: create → prepare → deploy (PBM) → tests → destroy
pytest test_pbm.py -v

# Show deploy and instance logs (no capture)
pytest test_pbm.py -s -v

# Keep instance after run (for debugging)
pytest test_pbm.py -s -v --keep-instance

# Specific platform (from platforms.yaml)
PLATFORM=ubuntu-jammy pytest test_pbm.py -v
PLATFORM=centos-7 VERSION=2.4.0 pytest test_pbm.py -v

# Vagrant (local, no AWS)
PLATFORM=ubuntu-jammy-vagrant pytest test_pbm.py -v
```

Required env for **EC2**: SSH private key at `~/.ssh/id_rsa`, or set `SSH_KEY_PATH` or `MOLECULE_AWS_PRIVATE_KEY`. Optional: `PLATFORM` (default `rhel9`).

### EC2 provisioner

- **Key pair**: Key name in AWS defaults to `molecule-pkg-tests`. Override with `EC2_KEY_NAME`, `MOLECULE_EC2_KEY_NAME`, or `ec2_key_name` in platform config. Private key path for SSH: `SSH_KEY_PATH`, then `MOLECULE_AWS_PRIVATE_KEY`, then `~/.ssh/id_rsa`.
- **Security groups**: If you set `vpc_subnet_id` and do **not** set `security_group_ids`, the provisioner creates or reuses a security group **`molecule-<vpc_id>`** with SSH (22), ICMP, and egress all. Set `security_group_ids` in platform config to use your own.
- **AWS credentials**: Use env vars, `~/.aws/credentials`, or IAM role.

---

## Deployment class (pyinfra API)

**`deployment.py`** uses the pyinfra **Python API only** (no CLI). It does not invoke `pyinfra` as a command; it builds a `State` and runs deploy functions programmatically.

- **Constructor**: `Deployment(inventory, config=None)` — takes a pyinfra `Inventory` (from `instance.inventory`) and an optional `Config` (defaults include `SUDO=True`, `CONNECT_TIMEOUT=60`).
- **Phases**: Each of `prepare()`, `setup_pbm()`, and `cleanup()` runs as a separate deploy. For each phase the class:
  1. Creates a **new** `State(inventory, config)` (pyinfra does not reuse one State for multiple deploy runs).
  2. Calls `connect_all(state)` to open SSH to all hosts in the inventory.
  3. Clears each host’s `op_hash_order` (hosts are reused across States; clearing avoids stale op hashes).
  4. Calls `add_deploy(state, deploy_func)` to register the deploy (e.g. `prepare.prepare` or `pbm_install.pbm_install`).
  5. Calls `run_ops(state)` to execute the operations.
  6. Calls `disconnect_all(state)` in a `finally` block so connections are always closed.

Deploy scripts live in `prepare.py`, `pbm_install.py`, and `cleanup.py`; each exposes a single `@deploy` function that is passed to `add_deploy`. Gevent is monkey-patched at import time so SSH I/O is non-blocking.

---

## Instance class (host provisioners)

**`instance.py`** creates and destroys the host(s) that tests run against. The **provider** is chosen from `platforms.yaml` via the `provider` key (default `ec2`).

- **`Instance.create(config)`** — `config` is a platform name (e.g. `rhel9`, `ubuntu-jammy`, `debian-12-vagrant`). It loads the platform from `platforms.yaml`, then:
  - **Vagrant** (`provider: vagrant`): Uses `cloud_vagrant.create_instances()`. Creates a temporary directory, runs Vagrant there, returns an `Instance` with connection details (address, port, user, SSH key). Teardown removes the temp dir and runs `vagrant destroy`.
  - **EC2** (default): Uses `cloud_ec2.create_instances()` with boto3. Launches instance(s) from the platform’s AMI/subnet/sg and returns an `Instance` with the same connection shape. Teardown terminates the instance(s) via the stored config.

- After create, **preflight** waits for SSH (TCP) to become reachable; if it times out, the instance is destroyed and creation fails.

- **Properties**:  
  - `instance.inventory` — pyinfra `Inventory` built from the connection list (used by `Deployment`).  
  - `instance.connection` — paramiko URI for the first host (used by testinfra).

- **`instance.destroy()`** — Calls the same provisioner (`cloud_vagrant` or `cloud_ec2`) to tear down the host(s) and clear temp dirs. The fixture calls this on teardown unless `--keep-instance` is set.

---

## platforms.yaml

**`platforms.yaml`** defines the OS/platforms you can run tests against. The fixture passes the platform name via the `PLATFORM` env var (default `rhel9`); `Instance.create(config)` and the provisioners read this file via `config.get_platform(name)`.

**Structure:**

- **`defaults`** — Shared keys merged into every platform (e.g. `region`, `vpc_subnet_id`, `instance_type`, `root_device_name`, `instance_tags`). A platform entry overrides any of these.
- **`platforms`** — Map of platform name → config. The name is what you pass as `PLATFORM` (e.g. `ubuntu-jammy`, `debian-12-vagrant`).

**Provider types:**

| provider   | Used by    | Main keys |
|-----------|------------|------------|
| `ec2`     | `cloud_ec2` | `image` (AMI), `instance_type`, `vpc_subnet_id`, `ssh_user`, `root_device_name`, `instance_tags`; optional `security_group_ids`, `instance_profile_arn`, `assign_public_ip`, `root_volume_size`, `root_volume_type`. |
| `vagrant` | `cloud_vagrant` | `box` (e.g. `ubuntu/jammy64`), optional `box_url`, `ssh_user`, `memory` (MB), `hostname`. |

**Env expansion:** Image IDs and similar values can use `${VAR:-default}` in the YAML; `config.py` expands them from the environment (e.g. `${MOLECULE_RHEL9_AMI:-ami-xxx}`).

---

## Adding a new OS (platform)

1. Edit **`platforms.yaml`**.
2. Under `platforms:`, add a new entry (see existing `ubuntu-jammy`, `rhel9`, etc.).
3. From the pyinfra directory: `PLATFORM=my-new-os pytest test_pbm.py -v`.

---

## Adding a new deploy script

1. Add a Python file at root (e.g. `my_install.py`) with a `@deploy` function.
2. In **`deployment.py`** add a method that calls `add_deploy(state, my_install.my_install)` and `run_ops(state)`.
3. Call that method from the **`prepare_instance`** fixture in the test module (e.g. `test_pbm.py`) if it should run before tests.

---

## Adding new tests

1. Add a test file at root (e.g. `test_my.py`).
2. Use the **`host`** fixture (defined in the test module, backed by `prepare_instance`). Example:

   ```python
   def test_something(host):
       with host.sudo("root"):
           result = host.run("systemctl status pbm-agent")
           assert result.rc == 0
   ```

---

## Dependencies between files

```
┌─────────────────┐     ┌─────────────────┐
│   test_pbm.py    │────▶│   instance.py   │
│  (fixtures,      │     │ Instance.create,│
│   tests)         │     │ .inventory,     │
└────────┬─────────┘     │ .connection,    │
         │               │ .destroy        │
         │               └────────┬────────┘
         │                        │
         │               ┌────────┴────────┐
         │               │                 │
         │               ▼                 ▼
         │        ┌──────────────┐  ┌─────────────┐
         │        │  config.py   │  │ inventory.py│
         │        │ get_platform │  │ inventory_  │
         │        └──────┬───────┘  │ from_       │
         │               │         │ connection_ │
         │               │         │ list        │
         │               ▼         └─────────────┘
         │        ┌──────────────┐
         │        │platforms.yaml│
         │        └──────────────┘
         │
         │        ┌─────────────────────────────────┐
         └───────▶│       deployment.py             │
                  │ Deployment(prepare, setup_pbm,  │
                  │            cleanup)             │
                  └────────────┬────────────────────┘
                               │
                  ┌────────────┼────────────┐
                  ▼            ▼            ▼
           ┌──────────┐ ┌────────────┐ ┌──────────┐
           │prepare.py│ │pbm_install.py│ │cleanup.py│
           └──────────┘ └────────────┘ └──────────┘

instance.py (at runtime, by provider):
         ┌─────────────┐
         │ instance.py │
         └──────┬──────┘
                │
    ┌───────────┴───────────┐
    ▼                       ▼
┌─────────────┐       ┌──────────────┐
│ cloud_ec2.py│       │cloud_vagrant.py│
└─────────────┘       └──────────────┘

conftest.py  ──▶ pytest only (hooks, options, log filter); no project imports.
```

- **test_pbm.py** → **instance**, **deployment**; fixtures use `Instance.create(config)` and `Deployment(instance.inventory)`.
- **instance.py** → **config** (`get_platform`); uses **cloud_ec2** or **cloud_vagrant** by provider; builds **inventory** via `inventory_from_connection_list`.
- **config.py** → reads **platforms.yaml**.
- **deployment.py** → runs **prepare**, **pbm_install**, **cleanup** (imported at run time).
- **conftest.py** → no project modules; pytest hooks only.

---

## Files overview

| File / dir        | Purpose |
|-------------------|--------|
| `instance.py`     | Instance lifecycle: create/destroy; `.inventory` (pyinfra), `.connection` (testinfra URI). |
| `deployment.py`   | Wrapper around pyinfra API: `prepare()`, `setup_pbm()`, `cleanup()`. |
| `config.py`       | Loads `platforms.yaml`. |
| `inventory.py`    | Builds pyinfra `Inventory` from connection list. |
| `platforms.yaml`  | OS/platform definitions (EC2, Vagrant). |
| `prepare.py`, `pbm_install.py`, `cleanup.py` | Pyinfra deploy scripts (`@deploy`). |
| `conftest.py`     | Pytest hooks (options, logging, collection). Fixtures live in test modules. |
| `test_pbm.py`, `test_*.py` | Testinfra tests (use `host` fixture). |
| `cloud_ec2.py`, `cloud_vagrant.py` | EC2 and Vagrant provisioning. |
