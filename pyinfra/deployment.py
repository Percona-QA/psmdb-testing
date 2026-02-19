"""
Deployment: wrapper around pyinfra Python API (no CLI). Runs prepare and setup_pbm in memory.
"""
from __future__ import annotations

import logging
import re
import sys

# Gevent monkey patch must run before any pyinfra/socket/paramiko use (avoids blocking SSH)
from gevent import monkey
monkey.patch_all()

from pyinfra.api import Config, State
from pyinfra.api.connect import connect_all, disconnect_all
from pyinfra.api.deploy import add_deploy
from pyinfra.api.operations import run_ops

# Suppress pyinfra beta/alpha and known_hosts warnings in logs
_SUPPRESS_LOG_PATTERNS = re.compile(
    r"currently in beta!|is in alpha!|is currently in beta|"
    r"No host key for .* found in known_hosts|"
    r"Added host key for .* to known_hosts",
    re.IGNORECASE,
)


class _SuppressPyinfraNoiseFilter(logging.Filter):
    """Drop pyinfra WARNING lines that are beta/alpha or known_hosts notices."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not record.name.startswith("pyinfra") or record.levelno != logging.WARNING:
            return True
        return _SUPPRESS_LOG_PATTERNS.search(record.getMessage()) is None


# Logging: only attach handler when logs should be visible (pytest -s, or not under pytest).
def _configure_logging():
    import os
    root = logging.getLogger()
    if root.handlers:
        return
    # When running under pytest without -s, capture is fd/sys/tee-sys → hide our logs
    capture = os.environ.get("PYINFRA_PYTEST_CAPTURE")
    if capture not in (None, "no"):
        return
    level = logging.INFO
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"))
    handler.addFilter(_SuppressPyinfraNoiseFilter())
    root.setLevel(level)
    root.addHandler(handler)
    logging.getLogger("pyinfra").setLevel(level)

_configure_logging()
logger = logging.getLogger("pyinfra.deployment")


class Deployment:
    """Runs pyinfra deploy functions (prepare, pbm_install) via API against an in-memory inventory."""

    def __init__(self, inventory, *, config: Config | None = None):
        self.inventory = inventory
        self.config = config or Config(SUDO=True, CONNECT_TIMEOUT=60)
        self._state: State | None = None

    def _run_deploy(self, deploy_func) -> None:
        """Create a fresh State for this deploy, connect, add deploy, run_ops, then disconnect."""
        deploy_name = getattr(deploy_func, "deploy_name", deploy_func.__name__)
        logger.info("Deploy phase: %s (creating state, connecting)", deploy_name)
        state = State(inventory=self.inventory, config=self.config)
        connect_all(state)
        logger.info("Connected to %s host(s); clearing op_hash_order", len(list(state.inventory)))
        # Hosts are reused across State instances; op_hash_order is not cleared by host.init().
        # Clear it so get_op_order() only sees ops from this deploy (avoids KeyError in op_meta).
        for host in state.inventory:
            host.op_hash_order.clear()
        try:
            logger.info("Adding deploy %s and running operations", deploy_name)
            add_deploy(state, deploy_func)
            run_ops(state)
            logger.info("Deploy %s finished successfully", deploy_name)
        finally:
            disconnect_all(state)
            logger.info("Disconnected after %s", deploy_name)

    def prepare(self) -> None:
        """Run prepare deploy (EPEL, base packages, etc.)."""
        import prepare as prepare_deploy

        logger.info("=== prepare() ===")
        self._run_deploy(prepare_deploy.prepare)

    def setup_pbm(self) -> None:
        """Run PBM install deploy (percona-release, PSMDB, PBM, pbm-agent)."""
        import pbm_install as pbm_install_deploy

        logger.info("=== setup_pbm() ===")
        self._run_deploy(pbm_install_deploy.pbm_install)

    def cleanup(self) -> None:
        """Run cleanup deploy if configured."""
        import cleanup as cleanup_deploy

        logger.info("=== cleanup() ===")
        self._run_deploy(cleanup_deploy.cleanup)

    def disconnect(self) -> None:
        """No-op (each phase disconnects its own state). Kept for API compatibility."""
        self._state = None
