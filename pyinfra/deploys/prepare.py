"""
Pyinfra deploy: prepare node (swap, EPEL, base packages).
Replaces playbooks/prepare.yml.
"""
from pyinfra.api import deploy
from pyinfra import host
from pyinfra.operations import apt, files, server, yum
from pyinfra.facts.server import LinuxDistribution


@deploy("Prepare node for PBM tests")
def prepare():
    dist = host.get_fact(LinuxDistribution)
    name = dist.get("name", "")
    major = int(dist.get("major") or 0)

    if name in ("CentOS", "RedHat", "Rocky", "AlmaLinux", "Amazon", "Amazon Linux"):
        if major == 8:
            server.shell(
                name="Create swap file",
                commands=["dd if=/dev/zero of=/swapfile bs=1M count=4096"],
                _sudo=True,
            )
            files.file(
                name="Set swap file permissions",
                path="/swapfile",
                mode="0600",
                user="root",
                group="root",
                _sudo=True,
            )
            server.shell(
                name="Format and enable swap",
                commands=["mkswap /swapfile", "swapon /swapfile"],
                _sudo=True,
            )
        if major == 8:
            server.shell(
                name="Install EPEL 8 GPG and repo",
                commands=[
                    "rpm --import https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-8",
                    "yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm",
                ],
                _sudo=True,
            )
        elif major == 7:
            server.shell(
                name="Install EPEL 7 GPG and repo",
                commands=[
                    "rpm --import https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-7",
                    "yum install -y https://dl.fedoraproject.org/pub/archive/epel/7/x86_64/Packages/e/epel-release-7-14.noarch.rpm",
                ],
                _sudo=True,
            )
        yum.packages(
            name="Install base packages (RHEL)",
            packages=["ca-certificates", "unzip", "wget", "rsync", "git"],
            present=True,
            _sudo=True,
        )
    elif name in ("Ubuntu", "Debian"):
        apt.packages(
            name="Install base packages (Debian)",
            packages=["unzip", "wget", "gnupg", "gnupg2", "rsync", "acl", "git"],
            update=True,
            present=True,
            _sudo=True,
        )


prepare()
