"""
Instance lifecycle: create/destroy in-memory; expose inventory (pyinfra) and connection (testinfra).
"""
from __future__ import annotations

import logging
import os
import socket
import tempfile
import time
from pathlib import Path

from config import get_platform

logger = logging.getLogger(__name__)

# Default SSH key path for EC2
def _default_key_path() -> str:
    return (
        os.environ.get("SSH_KEY_PATH")
        or os.environ.get("MOLECULE_AWS_PRIVATE_KEY")
        or str(Path("~/.ssh/id_rsa").expanduser())
    )


def _preflight_connect(
    address: str,
    port: int,
    timeout: float = 10.0,
    max_wait: float = 320.0,
    interval: float = 10.0,
) -> bool:
    """Wait for SSH (TCP) to become reachable. Returns True when reachable."""
    deadline = time.monotonic() + max_wait
    attempt = 0
    while True:
        attempt += 1
        logger.info("Preflight: TCP connect to %s:%s (attempt %s)...", address, port, attempt)
        try:
            sock = socket.create_connection((address, port), timeout=timeout)
            sock.close()
            logger.info("Preflight: OK (host reachable).")
            return True
        except Exception as e:
            if time.monotonic() >= deadline:
                logger.error("Preflight: host unreachable after %ss: %s", max_wait, e)
                return False
            logger.debug("Preflight: not yet reachable (%s), retrying in %ss...", e, interval)
            time.sleep(interval)


def _connection_to_paramiko_uri(inst: dict) -> str:
    """Build testinfra paramiko connection URI from one instance config dict."""
    from urllib.parse import urlencode

    addr = inst.get("address")
    user = inst.get("user", "root")
    port = int(inst.get("port", 22))
    key = (inst.get("identity_file") or "").strip()
    uri = f"paramiko://{user}@{addr}:{port}"
    if key:
        uri = f"{uri}?{urlencode({'ssh_identity_file': key})}"
    return uri


class Instance:
    """
    Holds connection list and destroy payload.
    .inventory: pyinfra Inventory (from inventory_from_connection_list).
    .connection: paramiko URI string for testinfra (first host).
    """

    def __init__(
        self,
        config_list: list[dict],
        provider: str,
        vagrant_temp_dir: Path | None = None,
    ):
        self._config_list = config_list
        self._provider = provider
        self._vagrant_temp_dir = vagrant_temp_dir
        self._inventory_temp_dir: Path | None = None
        self._inventory = None

    @classmethod
    def create(cls, config: str) -> Instance:
        """
        Create instance(s) for platform config (platform name from platforms.yaml).
        config: platform name, e.g. 'rhel9', 'ubuntu-jammy'.
        Returns Instance with connection list (Vagrant uses a temp dir for Vagrantfile).
        """
        logger.info("Creating instance for config=%s", config)
        platform_config = get_platform(config)
        if not platform_config:
            raise ValueError(f"Unknown platform: {config}")
        provider = platform_config.get("provider", "ec2")
        key_path = _default_key_path()
        logger.info("Provider=%s, SSH key=%s", provider, key_path)

        if provider == "vagrant":
            import cloud_vagrant

            vagrant_temp = Path(tempfile.mkdtemp(prefix="pyinfra_vagrant_"))
            try:
                config_list = cloud_vagrant.create_instances(
                    config,
                    platform_config,
                    key_path,
                    vagrant_dir=vagrant_temp,
                )
            except Exception:
                if vagrant_temp.exists():
                    import shutil
                    shutil.rmtree(vagrant_temp, ignore_errors=True)
                raise
            inst = cls(config_list, provider, vagrant_temp)
            first = config_list[0]
            if not _preflight_connect(first["address"], int(first.get("port", 22))):
                inst.destroy()
                raise RuntimeError("Preflight: SSH did not become reachable")
            logger.info("Instance created (vagrant): %s", first.get("address"))
            return inst

        else:
            import cloud_ec2

            if not Path(key_path).expanduser().exists():
                raise FileNotFoundError(
                    f"SSH private key not found: {key_path}. "
                    "Set SSH_KEY_PATH, MOLECULE_AWS_PRIVATE_KEY, or use ~/.ssh/id_rsa."
                )
            logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
            config_list = cloud_ec2.create_instances(
                config,
                platform_config,
                key_path,
            )
            inst = cls(config_list, provider, None)
            # Preflight: wait for SSH
            first = config_list[0]
            if not _preflight_connect(
                first["address"],
                int(first.get("port", 22)),
            ):
                inst.destroy()
                raise RuntimeError("Preflight: SSH did not become reachable")
            logger.info("Instance created (ec2): %s", first.get("address"))
            return inst

    @property
    def inventory(self):
        """Pyinfra Inventory from connection list (in-memory). Cached; temp dir cleaned in destroy()."""
        if self._inventory is None:
            from inventory import inventory_from_connection_list

            self._inventory, self._inventory_temp_dir = inventory_from_connection_list(
                self._config_list
            )
        return self._inventory

    @property
    def connection(self) -> str:
        """Paramiko URI for testinfra (first host)."""
        if not self._config_list:
            raise RuntimeError("No connection list")
        return _connection_to_paramiko_uri(self._config_list[0])

    def destroy(self) -> None:
        """Destroy instance(s) from in-memory config; remove temp dirs."""
        logger.info("Destroying instance(s) (provider=%s)", self._provider)
        if self._provider == "vagrant":
            import cloud_vagrant

            cloud_vagrant.destroy_instances_from_config(self._config_list)
            if self._vagrant_temp_dir and self._vagrant_temp_dir.exists():
                import shutil

                shutil.rmtree(self._vagrant_temp_dir, ignore_errors=True)
        else:
            import cloud_ec2

            logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
            cloud_ec2.destroy_instances_from_config(self._config_list)
        if self._inventory_temp_dir and self._inventory_temp_dir.exists():
            import shutil

            shutil.rmtree(self._inventory_temp_dir, ignore_errors=True)
        self._config_list = []
        self._inventory = None
        self._inventory_temp_dir = None
        logger.info("Instance destroyed")
