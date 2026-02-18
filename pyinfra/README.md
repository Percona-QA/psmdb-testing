# Pyinfra runner for package tests

This directory contains a **pyinfra-based test runner** that replaces Molecule + Ansible for package testing. It uses **boto3** for EC2, **pyinfra** for configuration, and **Vagrant** for local runs. Tests use **testinfra** over SSH (paramiko) with no Ansible.

## How it works

1. **Create** – Provision instance(s): EC2 (boto3) or Vagrant. Writes `instance_config.yml` and generates a pyinfra inventory in the **per-platform work dir**.
2. **Prepare** – Optional: run a prepare deploy (e.g. EPEL, base packages). Configured per suite.
3. **Deploy** – Run one or more deploy scripts (e.g. `deploys/pbm_install.py`) with pyinfra.
4. **Verify** – Run one or more test commands (e.g. `pytest tests/test_pbm.py -v`). `INSTANCE_CONFIG` points to the platform’s `instance_config.yml`.
5. **Cleanup** – Optional: run cleanup deploy. Configured per suite.
6. **Destroy** – Tear down instances (unless `--no-destroy`).

### Work directory layout

- **Single platform** (e.g. `python -m run ubuntu-jammy`): everything runs under **`.runner/<platform>/`** (e.g. `.runner/ubuntu-jammy/`). That folder holds `instance_config.yml`, `inventory.py`, `known_hosts`, and logs.
- **Parallel platforms** (e.g. `--platforms ubuntu-jammy,centos-7`): each platform runs in a separate process with `RUNNER_DIR=.runner/<platform>`, so each has its own `.runner/ubuntu-jammy/`, `.runner/centos-7/`, etc. No cross-platform conflicts.

### Running

From the **pyinfra** directory (no `PYTHONPATH` needed):

```bash
cd pyinfra

# Full run for one platform (create → prepare → deploy → verify → cleanup → destroy)
python -m run ubuntu-jammy

# Vagrant (local) – no AWS key
python -m run ubuntu-jammy-vagrant

# Only a specific stage (instance must exist for prepare/deploy/verify/cleanup)
python -m run ubuntu-jammy --stage deploy
python -m run ubuntu-jammy --stage verify

# Leave instance up after run
python -m run ubuntu-jammy --no-destroy

# Parallel: multiple platforms, each in its own .runner/<platform>/
python -m run --platforms ubuntu-jammy,centos-7,rhel9
python -m run --all-platforms --parallel 4

python -m run ubuntu-jammy
python -m run --platforms ubuntu-jammy,centos-7

# Different suite (from suites.yaml)
python -m run ubuntu-jammy --suite pbm

# List platforms / suites
python -m run --list-platforms
python -m run --list-suites
```

Required env for EC2 create: SSH private key at `~/.ssh/id_rsa`, or set `SSH_KEY_PATH` or `MOLECULE_AWS_PRIVATE_KEY`. Optional: `POST_PREFLIGHT_DELAY` (seconds after SSH is reachable before prepare/deploy).

### EC2 provisioner

- **Key pair**: Key name in AWS defaults to `molecule-pkg-tests`. Override with `EC2_KEY_NAME`, `MOLECULE_EC2_KEY_NAME`, or `ec2_key_name` in platform config. Private key path for SSH: `SSH_KEY_PATH`, then `MOLECULE_AWS_PRIVATE_KEY`, then `~/.ssh/id_rsa`. The key pair must already exist in AWS.
- **Security groups** (aligned with playbooks/create.yml): If you set `vpc_subnet_id` and do **not** set `security_group_ids`, the provisioner creates or reuses a security group named **`molecule-<vpc_id>`** (where `vpc_id` is derived from the subnet) with ingress: SSH (22), ICMP; egress: all. To use your own SGs instead, set `security_group_ids: [sg-xxx]` in the platform config. Optional: `security_group_name_prefix` in platform or `SECURITY_GROUP_NAME_PREFIX` in env (default `molecule`).
- **AWS credentials**: Use the usual mechanisms (env vars, `~/.aws/credentials`, IAM role).

---

## Adding a new OS (platform)

1. Edit **`platforms.yaml`**.
2. Under `platforms:`, add a new entry, e.g.:

   ```yaml
   my-new-os:
     provider: ec2    # or vagrant
     image: ami-xxxx  # EC2 AMI (or use env ${VAR:-default})
     ssh_user: ec2-user
     prepare: true    # run prepare deploy before main deploy
   ```

3. For **Vagrant**:

   ```yaml
   my-new-os-vagrant:
     provider: vagrant
     box: ubuntu/noble64
     ssh_user: vagrant
     prepare: true
     memory: 2048
   ```

4. Use `python -m run --list-platforms` to confirm. Run with `python -m run my-new-os` or `python -m run my-new-os-vagrant`.

---

## Adding a new suite

Suites define **prepare**, **deploy**, **verify**, and **cleanup** (paths and commands). One suite can be used for different products (e.g. PBM vs another product).

1. Edit **`suites.yaml`**.
2. Add a new entry under `suites:`:

   ```yaml
   my-suite:
     prepare: deploys/prepare.py          # or "none" to skip
     deploy: deploys/my_install.py        # or list: [ deploys/prepare.py, deploys/my_install.py ]
     verify:                              # one command (list of args) or list of commands
       - pytest
       - tests/test_my.py
       - "-v"
     cleanup: deploys/cleanup.py          # or "none" to skip
   ```

3. **Multiple deploys** (run in order):

   ```yaml
   deploy: [ deploys/prepare.py, deploys/my_install.py ]
   ```

4. **Multiple test commands** (each run in order; first failure fails the stage):

   ```yaml
   verify:
     - [ pytest, tests/test_my.py, "-v" ]
     - [ pytest, other/tests/test_other.py, "-v" ]
   ```

5. Run with `python -m run ubuntu-jammy --suite my-suite`. Use `--list-suites` to list names.

---

## Adding a new deploy script

1. Add a new Python file under **`deploys/`** (e.g. `deploys/my_install.py`).
2. Use pyinfra operations and the `@deploy` decorator; **call the deploy function at the end** of the file (e.g. `my_install()`).
3. Register it in **`suites.yaml`** in the desired suite as `deploy: deploys/my_install.py` or append it to a list.
4. Optional: add a **prepare** or **cleanup** script the same way and reference it in the suite.

Example minimal deploy:

```python
from pyinfra.api import deploy
from pyinfra import host
from pyinfra.operations import server

@deploy("My install")
def my_install():
    server.shell(name="Do something", commands=["echo done"], _sudo=True)

my_install()
```

---

## Adding new tests

1. **Tests under `pyinfra/tests/`** (recommended for this runner):  
   - Add a test file, e.g. **`pyinfra/tests/test_my.py`**.  
   - Use **testinfra** and the **`testinfra_hosts`** fixture. Hosts must come from **`INSTANCE_CONFIG`** (set by the runner to `.runner/<platform>/instance_config.yml`).  
   - Example pattern (see `pyinfra/tests/test_pbm.py`):

     ```python
     import os, yaml
     from urllib.parse import urlencode
     import testinfra

     def _testinfra_hosts_from_instance_config():
         config_path = os.environ.get("INSTANCE_CONFIG")
         if not config_path or not os.path.isfile(config_path):
             return []
         with open(config_path) as f:
             instances = yaml.safe_load(f) or []
         hosts = []
         for inst in instances:
             addr = inst.get("address")
             if not addr: continue
             user = inst.get("user", "root")
             port = int(inst.get("port", 22))
             key = (inst.get("identity_file") or "").strip()
             uri = f"paramiko://{user}@{addr}:{port}"
             if key:
                 uri = f"{uri}?{urlencode({'ssh_identity_file': key})}"
             hosts.append(uri)
         return hosts

     testinfra_hosts = _testinfra_hosts_from_instance_config()
     ```

   - Register in **`suites.yaml`** under `verify`, e.g. `[ pytest, tests/test_my.py, "-v" ]`, or add a second command in the `verify` list.

2. **Tests under `pbm/tests/` (or elsewhere)**  
   - Same idea: ensure `testinfra_hosts` is built from `INSTANCE_CONFIG` (connection URI strings).  
   - In the suite, point verify at that path, e.g. `[ pytest, pbm/tests/test_my.py, "-v" ]`.

3. **Multiple test files for one suite**  
   - In `suites.yaml` use a list of verify commands:

     ```yaml
     verify:
       - [ pytest, tests/test_pbm.py, "-v" ]
       - [ pytest, tests/test_other.py, "-v" ]
     ```

---

## Files overview

| File / dir        | Purpose |
|-------------------|--------|
| `run.py`          | CLI: create / prepare / deploy / verify / cleanup / destroy; single or parallel. |
| `config.py`       | Loads `platforms.yaml` and `suites.yaml`. |
| `inventory.py`    | Builds pyinfra inventory from `instance_config.yml`. |
| `pyinfra_runner.py` | Wrapper around pyinfra CLI (gevent fixes, empty-host handling). |
| `platforms.yaml`  | OS/platform definitions (EC2, Vagrant). |
| `suites.yaml`     | Suite definitions: prepare, deploy, verify, cleanup. |
| `deploys/*.py`    | Pyinfra deploy scripts. |
| `tests/test_*.py` | Testinfra tests (hosts from `INSTANCE_CONFIG`). |
| `cloud/vagrant.py`, `cloud/ec2.py` | Provisioning for Vagrant and EC2. |
| `.runner/<platform>/` | Per-platform work dir: `instance_config.yml`, `inventory.py`, logs. |

---

## Option reference

- `--stage create|prepare|deploy|verify|cleanup|destroy` – Run only that stage.  
- `--no-destroy` – Do not destroy instances after the run.  
- `--platforms A,B,C` – Run platforms in parallel; each uses `.runner/<platform>/`.  
- `--all-platforms` – Run all EC2 platforms in parallel.  
- `--parallel N` – Max concurrent platform runs (default: all when using `--platforms`/`--all-platforms`).  
- `--suite NAME` – Use suite from `suites.yaml` (default: `pbm`).   
- `--list-platforms` / `--list-suites` – List names and exit.
