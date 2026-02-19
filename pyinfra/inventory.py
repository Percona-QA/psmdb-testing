"""
Build pyinfra inventory from connection data (in-memory).
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

# In-memory builder uses pyinfra API
try:
    from pyinfra.api import Inventory as PyinfraInventory
except ImportError:
    PyinfraInventory = None  # type: ignore[misc, assignment]


def _connection_data_to_ssh_data(inst: dict, known_hosts_path: str) -> dict[str, Any]:
    """Build SSH connection data dict for one host (same options as file-based inventory)."""
    data: dict[str, Any] = {
        "ssh_user": inst.get("user", "root"),
        "ssh_port": int(inst.get("port", 22)),
        "ssh_known_hosts_file": known_hosts_path,
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
    return data


def inventory_from_connection_list(connection_list: list[dict]) -> tuple[Any, Path | None]:
    """
    Build a pyinfra Inventory from an in-memory connection list (no config file read).
    connection_list: list of dicts with address, user, port, identity_file (e.g. from cloud_ec2/cloud_vagrant create_instances).
    Returns (Inventory, temp_dir): temp_dir is a Path to a temp directory containing known_hosts (caller should
    remove it when done, e.g. in Instance.destroy).
    """
    if not connection_list:
        raise ValueError("Connection list is empty")
    if PyinfraInventory is None:
        raise RuntimeError("pyinfra is not installed")
    temp_dir = Path(tempfile.mkdtemp(prefix="pyinfra_inventory_"))
    known_hosts_path = temp_dir / "known_hosts"
    known_hosts_path.touch()
    known_hosts_str = str(known_hosts_path)
    # Pyinfra Inventory(names_data) expects tuple of (names, data). We use first host's data for all (single-host typical).
    names = [inst["address"] for inst in connection_list if inst.get("address")]
    if not names:
        raise ValueError("No valid address in connection list")
    data = _connection_data_to_ssh_data(connection_list[0], known_hosts_str)
    names_data = (names, data)
    inv = PyinfraInventory(names_data)
    return inv, temp_dir
