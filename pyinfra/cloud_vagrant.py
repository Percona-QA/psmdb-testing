"""
Vagrant lifecycle: create VM (vagrant up) and destroy (vagrant destroy) via python-vagrant. Config is in-memory only.
For local testing without EC2.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import vagrant

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
    *,
    vagrant_dir: str | Path,
    count: int = 1,
) -> list[dict]:
    """
    Bring up a Vagrant VM via python-vagrant, return instance config from v.ssh_config() (in-memory, no file).
    key_path is ignored (Vagrant uses its own key); kept for API compatibility with EC2.
    vagrant_dir: directory for Vagrantfile and .vagrant (must exist or will be created).
    """
    if count != 1:
        raise ValueError("Vagrant provider supports only count=1")
    vagrant_dir = Path(vagrant_dir)
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
    return [instance]


def destroy_instances_from_config(instance_config: list[dict]) -> None:
    """Destroy Vagrant VM(s) from in-memory config list (no file I/O). Uses vagrant_dir from each item."""
    if not instance_config:
        return
    for item in instance_config:
        vagrant_dir = item.get("vagrant_dir")
        if not vagrant_dir or not Path(vagrant_dir).exists():
            continue
        v = vagrant.Vagrant(vagrant_dir)
        v.destroy()
