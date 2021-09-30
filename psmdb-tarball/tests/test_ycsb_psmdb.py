import os
import sys
import pytest

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

def test_ycsb_load(host):
    cmd = 'cd /workdir/ycsb-mongodb-binding && ./bin/ycsb load mongodb -s -P workloads/workloadb  -p mongodb.auth="false"'
    with host.sudo():
        result = host.run(cmd)
    for line in result.stdout.splitlines():
        print(line)

def test_ycsb_run(host,capsys):
    cmd = 'cd /workdir/ycsb-mongodb-binding && ./bin/ycsb run mongodb -s -P workloads/workloadb  -p mongodb.auth="false"'
    with host.sudo():
        result = host.run(cmd)
    for line in result.stdout.splitlines():
        print(line)

