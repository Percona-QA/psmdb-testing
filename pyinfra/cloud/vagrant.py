"""
Vagrant lifecycle: create VM (vagrant up), write instance config from python-vagrant API; destroy (vagrant destroy).
For local testing without EC2. Uses the python-vagrant module (no vagrant CLI in subprocess).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import vagrant
import yaml

VAGRANTFILE_TEMPLATE = """# Generated for pyinfra local testing
Vagrant.configure("2") do |config|
  config.vm.box = "{box}"
  config.vm.hostname = "{hostname}"
  config.vm.network "private_network", type: "dhcp"
  config.ssh.insert_key = true
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "{memory}"
    vb.cpus = 2
  end
end
"""

VAGRANTFILE_WITH_BOX_URL = """# Generated for pyinfra local testing
Vagrant.configure("2") do |config|
  config.vm.box = "{box}"
  config.vm.box_url = "{box_url}"
  config.vm.hostname = "{hostname}"
  config.vm.network "private_network", type: "dhcp"
  config.ssh.insert_key = true
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "{memory}"
    vb.cpus = 2
  end
end
"""


def _parse_ssh_config(ssh_config: str) -> dict[str, str]:
    """Parse output of ssh_config() into a dict (HostName, User, Port, IdentityFile)."""
    out = {}
    for line in ssh_config.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if " " in line:
            key, val = line.split(None, 1)
            out[key] = val.strip('"')
    return out


def create_instances(
    platform_name: str,
    platform_config: dict[str, Any],
    key_path: str,
    out_config_path: str | Path,
    *,
    count: int = 1,
) -> list[dict]:
    """
    Bring up a Vagrant VM via python-vagrant, then write instance config from v.ssh_config() (HostName, Port, User, IdentityFile).
    key_path is ignored (Vagrant uses its own key); kept for API compatibility with EC2.
    """
    if count != 1:
        raise ValueError("Vagrant provider supports only count=1")
    out_config_path = Path(out_config_path)
    vagrant_dir = out_config_path.parent
    vagrant_dir.mkdir(parents=True, exist_ok=True)

    box = platform_config.get("box") or "ubuntu/jammy64"
    box_url = platform_config.get("box_url", "")
    hostname = platform_config.get("hostname") or platform_name.replace(".", "-")
    memory = str(platform_config.get("memory", 2048))

    if box_url:
        vagrantfile_content = VAGRANTFILE_WITH_BOX_URL.format(
            box=box, box_url=box_url, hostname=hostname, memory=memory
        )
    else:
        vagrantfile_content = VAGRANTFILE_TEMPLATE.format(
            box=box, hostname=hostname, memory=memory
        )
    (vagrant_dir / "Vagrantfile").write_text(vagrantfile_content)

    v = vagrant.Vagrant(str(vagrant_dir))
    v.up()

    # Use ssh_config() so we get the connectable HostName (127.0.0.1 with port forward),
    # not the VM's hostname which is not resolvable from the host.
    ssh_config_str = v.ssh_config()
    parsed = _parse_ssh_config(ssh_config_str)
    address = parsed.get("HostName", "127.0.0.1")
    port = int(parsed.get("Port", 22))
    user = parsed.get("User") or platform_config.get("ssh_user", "vagrant")
    identity_file = (parsed.get("IdentityFile") or "").strip('"')
    path_key = Path(identity_file) if identity_file else None
    if path_key and path_key.exists():
        identity_file = str(path_key.resolve())
    elif identity_file and vagrant_dir:
        path_key = (vagrant_dir / identity_file).resolve()
        if path_key.exists():
            identity_file = str(path_key)
    if not identity_file:
        for p in vagrant_dir.glob(".vagrant/machines/*/virtualbox/private_key"):
            identity_file = str(p.resolve())
            break

    instance = {
        "instance": hostname,
        "address": address,
        "user": user,
        "port": port,
        "identity_file": identity_file,
        "instance_id": f"vagrant-{platform_name}",
        "region": "local",
        "vagrant_dir": str(vagrant_dir.resolve()),
    }
    instance_configs = [instance]
    with open(out_config_path, "w") as f:
        yaml.dump(instance_configs, f, default_flow_style=False, sort_keys=False)
    return instance_configs


def destroy_instances(config_path: str | Path) -> None:
    """Destroy Vagrant VM(s) listed in instance config using python-vagrant."""
    config_path = Path(config_path)
    if not config_path.exists():
        return
    with open(config_path) as f:
        instance_conf = yaml.safe_load(f) or []
    if not instance_conf:
        return
    for item in instance_conf:
        vagrant_dir = item.get("vagrant_dir")
        if not vagrant_dir or not Path(vagrant_dir).exists():
            continue
        v = vagrant.Vagrant(vagrant_dir)
        v.destroy()
    with open(config_path, "w") as f:
        yaml.dump([], f)
