import os
import pytest

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

def test_ycsb_load(host,record_property):
    cmd = 'cd /workdir/ycsb-mongodb-binding && ./bin/ycsb load mongodb -s -P workloads/workloadb  -p mongodb.auth="false" | tail -n +2'
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
    for line in result.stdout.splitlines():
        name, descr, timing = line.split(',')
        fullname = name + ' ' + descr
        record_property(fullname, timing)

def test_ycsb_run(host,record_property):
    cmd = 'cd /workdir/ycsb-mongodb-binding && ./bin/ycsb run mongodb -s -P workloads/workloadb  -p mongodb.auth="false" | tail -n +2'
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout) 
    for line in result.stdout.splitlines():
        name, descr, timing = line.split(',')
        fullname = name + ' ' + descr
        record_property(fullname, timing)
  
