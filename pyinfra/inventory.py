"""
Write pyinfra inventory from instance config (single source of truth for SSH connection).
"""
from __future__ import annotations

from pathlib import Path

import yaml


def instance_config_to_pyinfra_inventory(
    instance_config_path: str | Path,
    out_inventory_path: str | Path,
) -> None:
    """
    Read instance config (list of {address, user, port, identity_file, ...})
    and write a pyinfra inventory .py file: all = [(host, {ssh_user, ssh_port, ssh_key}), ...].
    Sets accept-new and connect/banner timeouts to avoid "kex_exchange_identification: Connection closed" on slow or first connect.
    """
    config_path = Path(instance_config_path)
    out_path = Path(out_inventory_path)
    with open(config_path) as f:
        instances = yaml.safe_load(f) or []
    if not instances:
        raise ValueError("Instance config is empty")
    runner_dir = out_path.parent
    runner_dir.mkdir(parents=True, exist_ok=True)
    known_hosts_path = runner_dir / "known_hosts"
    known_hosts_path.touch()
    entries = []
    for inst in instances:
        addr = inst.get("address")
        if not addr:
            continue
        data = {
            "ssh_user": inst.get("user", "root"),
            "ssh_port": int(inst.get("port", 22)),
            "ssh_known_hosts_file": str(known_hosts_path),
            "ssh_strict_host_key_checking": "accept-new",
            "ssh_connect_retries": 3,
            "ssh_connect_retry_min_delay": 2,
            "ssh_connect_retry_max_delay": 5,
            "ssh_paramiko_connect_kwargs": {
                "timeout": 60,
                "banner_timeout": 60,
            },
        }
        key = inst.get("identity_file")
        if key:
            data["ssh_key"] = key
        entries.append((addr, data))
    lines = [
        "# Generated from instance config (pyinfra inventory)",
        "all = [",
    ]
    for host, data in entries:
        data_str = ", ".join(f"{repr(k)}: {repr(v)}" for k, v in data.items())
        lines.append(f"    ({repr(host)}, {{{data_str}}}),")
    lines.append("]")
    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    # So pyinfra uses 60s connect timeout when run with this inventory (avoids timeout=10 override)
    runner_config = runner_dir / "config.py"
    runner_config.write_text("# Pyinfra config for this inventory\nCONNECT_TIMEOUT = 60\n")
