import os
import random
import subprocess
from time import sleep

import requests
import urllib3

import json
import testinfra.utils.ansible_runner
from data_integrity_check import compare_data_rs


source = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('jenkins-pml-source')

destination = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('jenkins-pml-destination')

pml = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('jenkins-pml-mongolink')

collections = int(os.getenv("COLLECTIONS", default = 5))
datasize = int(os.getenv("DATASIZE", default = 100))
distribute = os.getenv("DISTRIBUTE", default = "false")
extraEnvVars = os.getenv("EXTRA_ENV_VARS", default = "")
TIMEOUT = int(os.getenv("TIMEOUT",default = 300))

def create_config(datasize, collections):
    string = []
    documentCount = int(datasize / collections)
    for x in range(collections):
        collectionName = f"collection{x}"
        string2 = {'database': 'test','collection': collectionName,'count': documentCount,'content': {'binary': {'type': 'binary','minLength': 1048576, 'maxLength': 1048576}}}
        string.append(string2)
    return string

def distribute_create_config(dataSize, collections):
    string = []
    distribution_chunks = split_datasize(collections)
    for x in range(collections):
        distribution = int(dataSize / 100 * distribution_chunks[x])
        collectionName = f"collection{x}"
        string2 = {'database': 'test','collection': collectionName,'count': distribution,'content': {'binary': {'type': 'binary','minLength': 1048576, 'maxLength': 1048576}}}
        string.append(string2)
    return string

def split_datasize(numberOfChunks):
    parts = []
    remaining = 100
    for x in range(numberOfChunks - 1):
        max_number = remaining - (numberOfChunks - len(parts) - 1)
        n = random.randint(1, max_number)
        parts.append(n)
        remaining -= n
    parts.append(remaining)
    return parts

def check_mongod_service(node):
    with node.sudo():
        service = node.service("mongod")
        assert service.is_running

def restart_mongod(node):
    with node.sudo():
        hostname = node.check_output('hostname -s')
        result = node.check_output('systemctl restart mongod')
    print('restarting mongod on ' + hostname)

def load_data(node,port):
    if distribute == "true":
        config = distribute_create_config(datasize, collections)
    else:
        config = create_config(datasize, collections)
    config_json = json.dumps(config, indent=4)
    node.run_test('echo \'' + config_json + '\' > /tmp/generated_config.json')
    node.check_output('mgodatagen --uri=mongodb://127.0.0.1:' + port + '/?replicaSet=rs -f /tmp/generated_config.json --batchsize 10')

def check_count_data(node,port):
    result = node.check_output("mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval 'db.binary.count()' --quiet | tail -1")
    print('count objects in collection: ' + result)
    return result

def obtain_pml_address(node):
    ipaddress = node.check_output(
        "ip -4 addr show scope global | grep inet | awk '{print $2}' | cut -d/ -f1 | head -n 1")
    return ipaddress

def confirm_collection_size(node, port, amountOfCollections, datasize):
    sizes = []
    total = 0
    for collection in range(amountOfCollections):
        result = node.check_output(
        "mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval 'db.collection" + str(collection) + ".dataSize() / (1024 * 1024)' --quiet")
        sizes.append(int(float(result.strip())))
    for size in sizes:
        total += size
    if total == datasize:
        return True
    else:
        return False

def collect_cpu_useage(node):
    pmlAddress = obtain_pml_address(pml)
    return node.check_output('sudo curl -sk -u admin:admin "https://' + pmlAddress + '/prometheus/api/v1/query_range?query=100%20-%20(avg%20by(node_name)%20(rate(node_cpu_seconds_total%7Bmode%3D%22idle%22%7D%5B2m%5D))%20*%20100)&start=$(date -u -d \'10 minutes ago\' +%s)&end=$(date -u +%s)&step=15"')

def collect_memory_useage(node):
    pmlAddress = obtain_pml_address(pml)
    return node.check_output('curl -sk -u admin:admin "https://' + pmlAddress + '/prometheus/api/v1/query_range?query=100%20*%20(1%20-%20(node_memory_MemAvailable_bytes%20%2F%20node_memory_MemTotal_bytes))&start=$(date -u -d \'10 minutes ago\' +%s)&end=$(date -u +%s)&step=15"')

def wait_for_pml_replication(timeout=120):
    for _ in range(timeout):
        try:
            if "Data Clone completed" in pml.check_output('journalctl --grep="Data Clone completed"'):
                return True
        except subprocess.CalledProcessError:
            pass
        sleep(1)

    print("Timeout waiting for PML replication.")
    return False

def test_prepare_data():
    load_data(source,"27017")
    assert confirm_collection_size(source, "27017", collections, datasize)

def test_initiate_pml():
    result = pml.check_output(
        "percona-mongolink start")
    output = json.loads(result)
    assert output in [{"ok": True}, {'error': 'already running', 'ok': False}]
    assert wait_for_pml_replication()

def test_data_transfer():
    assert confirm_collection_size(destination, "27017", collections, datasize)

def test_data_integrity():
    assert compare_data_rs(source, destination, "27017")

