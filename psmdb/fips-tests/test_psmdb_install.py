import os
import pytest
import testinfra

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(os.environ["MOLECULE_INVENTORY_FILE"]).get_hosts("all")


BINARIES = [
    "mongod",
    "mongos",
    "bsondump",
    "mongoexport",
    "mongobridge",
    "mongofiles",
    "mongoimport",
    "mongorestore",
    "mongotop",
    "mongostat",
]

PSMDB_VER = os.environ.get("PSMDB_VERSION")


def test_mongod_service(host):
    mongod = host.service("mongod")
    assert mongod.is_running


@pytest.mark.parametrize("binary", BINARIES)
def test_binary_version(host, binary):
    result = host.run(f"{binary} --version")
    assert PSMDB_VER in result.stdout, result.stdout


def test_fips(host):
    with host.sudo():
        print(host.system_info.distribution)
        print(host.system_info.release)
        fips = host.check_output("cat /proc/sys/crypto/fips_enabled")
        assert fips == "1", fips
        if host.system_info.distribution == "debian" or host.system_info.distribution == "ubuntu":
            logs = host.check_output("head -n10 /var/log/mongodb/mongod.log")
        else:
            logs = host.check_output("head -n10 /var/log/mongo/mongod.log")
        print(logs)
        assert "FIPS 140-2 mode activated" in logs
