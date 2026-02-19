"""
Pytest hooks for pyinfra runner tests. Fixtures live in each test module.
All commands are intended to be run from the pyinfra directory (e.g. cd pyinfra && pytest test_pbm.py -v).
"""
from __future__ import annotations

import logging
import os
import re
import sys


def pytest_addoption(parser):
    """Add --keep-instance so instance is not destroyed after tests (for debugging)."""
    group = parser.getgroup("pyinfra", "pyinfra instance lifecycle")
    group.addoption(
        "--keep-instance",
        action="store_true",
        default=False,
        help="Do not destroy the instance after test run (default: always destroy).",
    )


# Patterns for pyinfra log messages to suppress (beta/alpha, known_hosts noise)
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


def pytest_configure(config):
    """Show deployment/instance/pyinfra logs only when -s (no capture) is used; otherwise hide them."""
    capture = getattr(config.option, "capture", "fd")
    os.environ["PYINFRA_PYTEST_CAPTURE"] = capture
    pyinfra_log = logging.getLogger("pyinfra")
    if capture != "no":
        # Suppress pyinfra WARNING/INFO so they don't appear in "Captured log setup"
        pyinfra_log.setLevel(logging.ERROR)
        return
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"))
    handler.addFilter(_SuppressPyinfraNoiseFilter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    pyinfra_log.setLevel(logging.INFO)


def pytest_collection_modifyitems(config, items):
    """Replace testinfra host id in nodeid with platform name for clearer reports."""
    platform = os.environ.get("PLATFORM_NAME", "").strip()
    if not platform:
        return
    for item in items:
        # nodeid is like "test_pbm.py::test_package[paramiko://127.0.0.1]"
        if "[paramiko://" in item.nodeid:
            new_nodeid = re.sub(r"\[[^\]]+\]$", f"[{platform}]", item.nodeid)
            if new_nodeid != item.nodeid:
                item._nodeid = new_nodeid  # noqa: SLF001
