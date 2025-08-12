import os
import pytest
import requests
from packaging import version
import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

DEB_PACKAGES = ['percona-server-mongodb', 'percona-server-mongodb-server', 'percona-server-mongodb-mongos',
                'percona-server-mongodb-tools', 'percona-server-mongodb-dbg']
RPM_PACKAGES_7 = ['percona-server-mongodb', 'percona-server-mongodb-server', 'percona-server-mongodb-mongos',
                'percona-server-mongodb-tools', 'percona-server-mongodb-debuginfo']
RPM_PACKAGES_8 = ['percona-server-mongodb', 'percona-server-mongodb-server', 'percona-server-mongodb-mongos',
                           'percona-server-mongodb-tools',
                           'percona-server-mongodb-mongos-debuginfo',
                           'percona-server-mongodb-server-debuginfo',
                           'percona-server-mongodb-tools-debuginfo']

RPM_PACKAGES_9 = ['percona-server-mongodb', 'percona-server-mongodb-server', 'percona-server-mongodb-mongos',
                'percona-server-mongodb-tools']

BINARIES = ['mongod', 'mongos', 'bsondump', 'mongoexport', 'mongobridge',
            'mongofiles', 'mongoimport', 'mongorestore', 'mongotop', 'mongostat']

PSMDB_VER = os.environ.get("PDMDB_VERSION").lstrip("pdmdb-")
TESTING_BRANCH = os.environ.get("TESTING_BRANCH")
MONGOSH_VER_RHEL7 = '2.1.5'

def get_mongosh_ver():
    url = "https://raw.githubusercontent.com/Percona-QA/psmdb-testing/" + TESTING_BRANCH + "/MONGOSH_VERSION"
    r = requests.get(url)
    return r.text.rstrip()

MONGOSH_VER = get_mongosh_ver()

def test_mongod_service(host):
    mongod = host.service("mongod")
    assert mongod.is_running


@pytest.mark.parametrize("package", DEB_PACKAGES)
def test_deb_packages(host, package):
    os = host.system_info.distribution
    if os.lower() in ["redhat", "centos", "rhel", "amzn"]:
        pytest.skip("This test only for Debian based platforms")
    pkg = host.package(package)
    assert pkg.is_installed
    assert PSMDB_VER in pkg.version


# TODO add check that minor version is correct
@pytest.mark.parametrize("package", RPM_PACKAGES_7)
def test_rpm7_packages(host, package):
    os = host.system_info.distribution
    if os in ["debian", "ubuntu", "amzn"]:
        pytest.skip("This test only for RHEL based platforms")
    if float(host.system_info.release) != 7.0:
        pytest.skip("Only for centos7 tests")
    pkg = host.package(package)
    assert pkg.is_installed
    assert PSMDB_VER in pkg.version


@pytest.mark.parametrize("package", RPM_PACKAGES_8)
def test_rpm8_packages(host, package):
    os = host.system_info.distribution
    if os in ["debian", "ubuntu", "amzn"]:
        pytest.skip("This test only for RHEL based platforms")
    if float(host.system_info.release) != 8.0:
        pytest.skip("Only for centos8 tests")
    pkg = host.package(package)
    assert pkg.is_installed
    assert PSMDB_VER in pkg.version

@pytest.mark.parametrize("package", RPM_PACKAGES_9)
def test_rpm9_packages(host, package):
    os = host.system_info.distribution
    if os in ["debian", "ubuntu", "amzn"]:
        pytest.skip("This test only for RHEL based platforms")
    if float(host.system_info.release) < 9.0:
        pytest.skip("Only for centos9 tests")
    pkg = host.package(package)
    assert pkg.is_installed
    assert PSMDB_VER in pkg.version

@pytest.mark.parametrize("package", RPM_PACKAGES_8)
def test_al_packages(host, package):
    os = host.system_info.distribution
    if os in ["debian", "ubuntu", "redhat", "centos", "rhel"]:
        pytest.skip("This test only for AL platforms")
    pkg = host.package(package)
    assert pkg.is_installed
    assert PSMDB_VER in pkg.version

@pytest.mark.parametrize("binary", BINARIES)
def test_binary_version(host, binary):
    result = host.run(f"{binary} --version")
    assert PSMDB_VER in result.stdout, result.stdout

def test_cli_version(host):
    result = host.check_output("mongo --version")
    if version.parse(PSMDB_VER) > version.parse("6.0.0"):
        if host.system_info.distribution.lower() in ["redhat", "centos", 'rhel'] and host.system_info.release == '7':
           assert MONGOSH_VER_RHEL7 in result
        else:
           assert MONGOSH_VER in result
    else:
        assert PSMDB_VER in result

def test_telemetry(host):
    file_path = "/usr/local/percona/telemetry_uuid"
    expected_fields = ["instanceId", "PRODUCT_FAMILY_PSMDB"]
    expected_group = "percona-telemetry"

    assert host.file(file_path).exists, f"Telemetry file '{file_path}' does not exist."

    file_content = host.file(file_path).content_string
    for string in expected_fields:
        assert string in file_content, f"Field '{string}' wasn't found in file '{file_path}'."

    if not (host.system_info.distribution.lower() in ["redhat", "centos", 'rhel'] and host.system_info.release == '7'):
        file_group = host.file(file_path).group
        assert file_group == expected_group, f"File '{file_path}' group is '{file_group}', expected group is '{expected_group}'."
