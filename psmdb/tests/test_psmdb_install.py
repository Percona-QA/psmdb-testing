import os
import pytest
import requests
from packaging import version
import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')


BINARIES = ['mongod', 'mongos', 'bsondump', 'mongoexport', 'mongobridge',
            'mongofiles', 'mongoimport', 'mongorestore', 'mongotop', 'mongostat']

PSMDB_VER = os.environ.get("PSMDB_VERSION")
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

def test_telemetry_service(host):
    parts = PSMDB_VER.split(".")
    major_version = ".".join(parts[:-1])
    match major_version:
        case "5.0":
         if version.parse(PSMDB_VER) <= version.parse("5.0.27"):
          pytest.skip("This version doesn't support telemetry")
        case "6.0":
         if version.parse(PSMDB_VER) <= version.parse("6.0.15"):
          pytest.skip("This version doesn't support telemetry")
        case "7.0":
         if version.parse(PSMDB_VER) <= version.parse("7.0.11"):
          pytest.skip("This version doesn't support telemetry")
    if not (host.system_info.distribution.lower() in ["redhat", "centos", 'rhel'] and host.system_info.release == '7'):
        telemetry = host.service("percona-telemetry-agent")
        assert telemetry.is_running

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

