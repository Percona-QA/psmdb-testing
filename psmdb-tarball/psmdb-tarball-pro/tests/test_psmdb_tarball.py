import os
import re
import pytest

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')


BINARIES = ['mongod', 'mongos', 'bsondump', 'mongoexport', 'mongobridge',
            'mongofiles', 'mongoimport', 'mongorestore', 'mongotop', 'mongostat']

tarball_url = os.environ["TARBALL_OL9"]
match_exp_version = re.search(r'server-mongodb-pro-(\d+\.\d+\.\d+-\d+)', tarball_url)

JSTESTS = ['test_kerberos_simple.js','test_ldap_simple.js']
SUITES = ['ssl jstests/ssl/ssl_fips.js']

@pytest.mark.parametrize("binary", BINARIES)
def test_binary_version(host, binary):
    result = host.check_output(f"/usr/bin/{binary} --version")
    assert match_exp_version.group(1) in result, f"{result}"

@pytest.mark.parametrize("jstest", JSTESTS)
def test_jstests(host, jstest):
    cmd = "cd /percona-server-mongodb && python3 buildscripts/resmoke.py run /package-testing/jstests/"  + jstest
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
        print(result.stdout)
    assert result.rc == 0, result.stdout

@pytest.mark.parametrize("suites", SUITES)
def test_suites(host, suites):
    if host.system_info.distribution == "debian":
	pytest.skip("Skip debian12 as no openssl with FIPS available")
    cmd = "cd /percona-server-mongodb && python3 buildscripts/resmoke.py run --suite "  + suites
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
        print(result.stdout)
    assert result.rc == 0, result.stdout
