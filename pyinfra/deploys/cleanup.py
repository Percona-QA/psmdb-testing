"""
Pyinfra deploy: remove PBM package (optional teardown step).
Replaces pbm/install/playbooks/cleanup.yml.
"""
from pyinfra import host
from pyinfra.api import deploy
from pyinfra.operations import apt, yum
from pyinfra.facts.server import LinuxDistribution


@deploy("Remove PBM package")
def cleanup():
    dist = host.get_fact(LinuxDistribution)
    name = dist.get("name", "")
    if name in ("Ubuntu", "Debian"):
        apt.packages(
            name="Remove PBM (Debian)",
            packages=["percona-backup-mongodb"],
            present=False,
            _sudo=True,
        )
    elif name in ("CentOS", "RedHat", "Rocky", "AlmaLinux", "Amazon", "Amazon Linux"):
        yum.packages(
            name="Remove PBM (RHEL)",
            packages=["percona-backup-mongodb"],
            present=False,
            _sudo=True,
        )
