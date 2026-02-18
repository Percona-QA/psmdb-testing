"""
Pyinfra deploy: install Percona release, PSMDB, PBM, configure pbm-agent.
Replaces pbm/install/tasks/main.yml and included tasks.
"""
import os

from pyinfra import host
from pyinfra.api import deploy
from pyinfra.operations import apt, files, server, yum
from pyinfra.facts.server import Arch, LinuxDistribution


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


@deploy("Install PBM and PSMDB")
def pbm_install():
    dist = host.get_fact(LinuxDistribution)
    name = dist.get("name", "")
    major = int(dist.get("major") or 0)
    arch = host.get_fact(Arch) or ""

    psmdb_to_test = _env("psmdb_to_test", "psmdb-80")
    install_repo = _env("install_repo", "main")
    prel_version = _env("PREL_VERSION", "latest")
    is_debian = name in ("Ubuntu", "Debian")
    is_redhat = name in ("CentOS", "RedHat", "Rocky", "AlmaLinux", "Amazon", "Amazon Linux")
    is_arm = arch in ("aarch64", "arm64")

    if is_debian:
        if prel_version != "latest":
            apt.deb(
                name="Install percona-release 1.0-27 (Debian)",
                src="https://repo.percona.com/apt/percona-release_1.0-27.generic_all.deb",
                present=True,
                _sudo=True,
            )
        else:
            apt.deb(
                name="Install percona-release latest (Debian)",
                src="https://repo.percona.com/apt/percona-release_latest.generic_all.deb",
                present=True,
                _sudo=True,
            )
    elif is_redhat:
        if prel_version != "latest":
            server.shell(
                name="Install percona-release 1.0-27 (RHEL)",
                commands=["rpm -ivh --nodigest --nofiledigest https://repo.percona.com/yum/percona-release-1.0-27.noarch.rpm"],
                _sudo=True,
            )
            if is_arm:
                server.shell(
                    name="Add ARM support to percona-release",
                    commands=['sed -i "s|x86_64|x86_64 aarch64|" /usr/bin/percona-release'],
                    _sudo=True,
                )
        else:
            server.shell(
                name="Install percona-release latest (RHEL)",
                commands=["yum -y install https://repo.percona.com/yum/percona-release-latest.noarch.rpm"],
                _sudo=True,
            )
        server.shell(name="Clean yum cache", commands=["yum clean all"], _sudo=True)

    server.shell(
        name="Enable PSMDB repo",
        commands=[f"percona-release enable {psmdb_to_test} testing"],
        _sudo=True,
    )
    if install_repo == "main":
        server.shell(name="Enable PBM repo (main)", commands=["percona-release enable pbm"], _sudo=True)
    elif install_repo == "tools":
        server.shell(name="Enable tools repo", commands=["percona-release enable tools release"], _sudo=True)
    else:
        server.shell(
            name="Enable PBM repo (custom)",
            commands=[f"percona-release enable pbm {install_repo}"],
            _sudo=True,
        )

    psmdb_mongosh = psmdb_to_test.split("-")[1] if "-" in psmdb_to_test else "60"
    psmdb_packages = [
        "percona-server-mongodb",
        "percona-server-mongodb-server",
        "percona-server-mongodb-mongos",
        "percona-server-mongodb-tools",
    ]
    env_telemetry = {"PERCONA_TELEMETRY_URL": "https://check-dev.percona.com/v1/telemetry/GenericReport"}

    if is_debian:
        apt.update()
        apt.packages(
            name="Install PSMDB packages (Debian)",
            packages=psmdb_packages,
            update=True,
            present=True,
            _env=env_telemetry,
            _sudo=True,
        )
        if int(psmdb_mongosh) >= 60:
            apt.packages(
                name="Install mongosh (Debian)",
                packages=["percona-mongodb-mongosh"],
                update=True,
                present=True,
                _sudo=True,
            )
        else:
            apt.packages(
                name="Install mongo shell (Debian)",
                packages=["percona-server-mongodb-shell"],
                update=True,
                present=True,
                _sudo=True,
            )
    elif is_redhat:
        yum.packages(
            name="Install PSMDB packages (RHEL)",
            packages=psmdb_packages,
            present=True,
            _env=env_telemetry,
            _sudo=True,
        )
        if int(psmdb_mongosh) >= 60:
            yum.packages(
                name="Install mongosh (RHEL)",
                packages=["percona-mongodb-mongosh"],
                present=True,
                _sudo=True,
            )
        else:
            yum.packages(
                name="Install mongo shell (RHEL)",
                packages=["percona-server-mongodb-shell"],
                present=True,
                _sudo=True,
            )

    if int(psmdb_mongosh) >= 60:
        server.shell(
            name="Link mongo to mongosh",
            commands=["ln -sf /usr/bin/mongosh /usr/bin/mongo"],
            _sudo=True,
        )

    server.service(name="Stop mongod", service="mongod", running=False, _sudo=True)
    files.block(
        name="Set replica set in mongod.conf",
        path="/etc/mongod.conf",
        content='replication:\n  replSetName: "rs1"',
        line=r"^#replication:",
        after=True,
        _sudo=True,
    )
    server.service(name="Start mongod", service="mongod", running=True, enabled=True, _sudo=True)
    server.shell(
        name="Wait for mongod and initiate replica set",
        commands=[
            "for i in $(seq 1 20); do mongo --eval='printjson(db.serverStatus().ok)' 2>/dev/null | grep -q 1 && break; sleep 1; done",
            "mongo --eval 'rs.initiate()'",
        ],
        _sudo=True,
    )

    if is_debian:
        apt.packages(
            name="Install PBM (Debian)",
            packages=["percona-backup-mongodb"],
            update=True,
            present=True,
            _sudo=True,
        )
    elif is_redhat:
        yum.packages(
            name="Install PBM (RHEL)",
            packages=["percona-backup-mongodb"],
            present=True,
            _sudo=True,
        )

    aws_key = _env("AWS_ACCESS_KEY_ID", "")
    aws_secret = _env("AWS_SECRET_ACCESS_KEY", "")
    gcp_key = _env("GCP_ACCESS_KEY", "")
    gcp_secret = _env("GCP_SECRET_KEY", "")

    s3_content = f"""storage:
  type: s3
  s3:
    region: us-east-1
    bucket: operator-testing
    credentials:
      access-key-id: {aws_key}
      secret-access-key: {aws_secret}
"""
    files.block(
        name="Create pbm-agent S3 storage config",
        path="/etc/pbm-agent-storage.conf",
        content=s3_content,
        _sudo=True,
    )
    server.shell(
        name="Set pbm-agent S3 config permissions",
        commands=["chmod 0666 /etc/pbm-agent-storage.conf"],
        _sudo=True,
    )
    gcp_content = f"""storage:
  type: s3
  s3:
    region: us-east-1
    endpointUrl: https://storage.googleapis.com
    bucket: operator-testing
    credentials:
      access-key-id: {gcp_key}
      secret-access-key: {gcp_secret}
"""
    files.block(
        name="Create pbm-agent GCP storage config",
        path="/etc/pbm-agent-storage-gcp.conf",
        content=gcp_content,
        _sudo=True,
    )
    server.shell(
        name="Set pbm-agent GCP config permissions",
        commands=["chmod 0666 /etc/pbm-agent-storage-gcp.conf"],
        _sudo=True,
    )
    local_content = """storage:
  type: filesystem
  filesystem:
    path: /tmp
"""
    files.block(
        name="Create pbm-agent local storage config",
        path="/etc/pbm-agent-storage-local.conf",
        content=local_content,
        _sudo=True,
    )
    server.shell(
        name="Set pbm-agent local config permissions",
        commands=["chmod 0666 /etc/pbm-agent-storage-local.conf"],
        _sudo=True,
    )

    pbm_uri = 'PBM_MONGODB_URI="mongodb://localhost:27017/"'
    if is_debian:
        files.line(
            name="Set pbm-agent MongoDB URI (Debian)",
            path="/etc/default/pbm-agent",
            line=pbm_uri,
            _sudo=True,
        )
    elif is_redhat:
        files.line(
            name="Set pbm-agent MongoDB URI (RHEL)",
            path="/etc/sysconfig/pbm-agent",
            line=pbm_uri,
            _sudo=True,
        )

    server.service(
        name="Start and enable pbm-agent",
        service="pbm-agent",
        running=True,
        enabled=True,
        _sudo=True,
    )


pbm_install()
