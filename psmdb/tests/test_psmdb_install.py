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

def get_mongosh_ver():
    url = "https://raw.githubusercontent.com/Percona-QA/psmdb-testing/" + TESTING_BRANCH + "/MONGOSH_VERSION"
    r = requests.get(url)
    return r.text

MONGOSH_VER = get_mongosh_ver()

def test_mongod_service(host):
    mongod = host.service("mongod")
    assert mongod.is_running


@pytest.mark.parametrize("binary", BINARIES)
def test_binary_version(host, binary):
    result = host.run(f"{binary} --version")
    assert PSMDB_VER in result.stdout, result.stdout

def test_cli_version(host):
    result = host.check_output("mongo --version")
    if version.parse(PSMDB_VER) > version.parse("6.0.0"):
        assert MONGOSH_VER in result
    else:
        assert PSMDB_VER in result

def test_loaded_data(host):
    cmd = "/package-testing/scripts/mongo_check.sh"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

