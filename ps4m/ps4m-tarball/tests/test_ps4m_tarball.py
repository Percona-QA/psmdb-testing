import os

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

RESMOKE_SUITE = 'mongot_community_fixture_e2e_single_node'


def test_mongot_community_fixture_e2e_single_node(host):
    cmd = (
        "cd /percona-server-mongodb && "
        "/opt/venv/bin/python buildscripts/resmoke.py run "
        f"--suite {RESMOKE_SUITE} --installDir /usr/bin --continueOnFailure"
    )
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
    assert result.rc == 0, result.stdout
