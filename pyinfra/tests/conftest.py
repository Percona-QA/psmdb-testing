"""
Pytest hooks for pyinfra runner tests.
Rewrites test report to show platform name (from PLATFORM_NAME) instead of paramiko://host.
"""
from __future__ import annotations

import os
import re


def pytest_collection_modifyitems(config, items):
    """Replace testinfra host id in nodeid with platform name for clearer reports."""
    platform = os.environ.get("PLATFORM_NAME", "").strip()
    if not platform:
        return
    for item in items:
        # nodeid is like "tests/test_pbm.py::test_package[paramiko://127.0.0.1]"
        if "[paramiko://" in item.nodeid:
            new_nodeid = re.sub(r"\[[^\]]+\]$", f"[{platform}]", item.nodeid)
            if new_nodeid != item.nodeid:
                item._nodeid = new_nodeid  # noqa: SLF001
