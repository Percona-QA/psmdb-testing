import os
import pytest

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')


SUITES = ['core','backup_tde_gcm','backup_tde_cbc']

@pytest.mark.parametrize("suite", SUITES)
def test_functional(host, suite):
    cmd = "cd /percona-server-mongodb && python3 buildscripts/resmoke.py run --suite" + ' '  + suite 
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

