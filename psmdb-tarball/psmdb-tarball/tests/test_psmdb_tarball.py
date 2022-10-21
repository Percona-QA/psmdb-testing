import os
import pytest

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')


SUITES = ['core']
JSTESTS = ['test_kerberos_simple.js','test_ldap_simple.js']

@pytest.mark.parametrize("jstest", JSTESTS)
def test_jstests(host, jstest):
    cmd = "cd /percona-server-mongodb && python3 buildscripts/resmoke.py run /psmdb-testing/jstests/"  + jstest 
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
    assert result.rc == 0, result.stdout

@pytest.mark.parametrize("suite", SUITES)
def test_suites(host, suite):
    cmd = "cd /percona-server-mongodb && python3 buildscripts/resmoke.py run --suite" + ' '  + suite 
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
    assert result.rc == 0, result.stdout

