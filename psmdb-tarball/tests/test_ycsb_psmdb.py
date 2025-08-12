import os

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

layout = os.environ.get("LAYOUT_TYPE")

def test_ycsb_run(host):
    if layout == "replicaset":
        cmd = 'cd /workdir/ycsb-mongodb-binding && ./bin/ycsb run mongodb -s -P workloads/workloadb -p recordcount=10000 -p operationcount=10000 -p mongodb.url="mongodb://localhost:27017/ycsb?replicaSet=rs0&w=1" -p mongodb.auth="false"'
    else:
        cmd = 'cd /workdir/ycsb-mongodb-binding && ./bin/ycsb run mongodb -s -P workloads/workloadb -p recordcount=10000 -p operationcount=10000 -p mongodb.auth="false"'
    with host.sudo():
        result = host.run(cmd)
    for line in result.stdout.splitlines():
        print(line)

