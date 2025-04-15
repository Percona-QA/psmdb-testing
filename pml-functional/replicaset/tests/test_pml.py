import os
import random
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

def collect_cpu_useage():
    urllib3.disable_warnings()

    url = "https://<PMM_SERVER>/prometheus/api/v1/query"
    query = '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[10m])) * 100)'
    resp = requests.get(url, params={"query": query}, auth=("admin", "admin"), verify=False)

    data = resp.json()["data"]["result"]
    for node in data:
        instance = node["metric"]["instance"]
        cpu_usage = float(node["value"][1])
        print(f"{instance}: {cpu_usage:.2f}% CPU usage (last 10 min)")


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

def drop_database(node,port):
    result = node.check_output("mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval 'db.dropDatabase()' --quiet")
    print(result)

def obtain_pml_address(node):
    ipaddress = node.check_output(
        "ip -4 addr show scope global | grep inet | awk '{print $2}' | cut -d/ -f1 | head -n 1")
    return ipaddress

def collect_cpu_useage(node, ipaddress):
    pmmexporterid = node.check_output("pmm-admin list | awk '/node_exporter/ {print $4}'")
    print("KEITH TEST: " + pmmexporterid )
    print("KEITH TEST 2: " + node.check_output('sudo curl -sk -u admin:admin "https://${ipaddress}/prometheus/api/v1/query?query=100%20-%20(rate(node_cpu_seconds_total%7Bmode%3D%22idle%22%2C%20instance%3D%22' + pmmexporterid + '%22%7D%5B10m%5D)%20*%20100)"'))


# def test_prepare_data():
#     load_data(source,"27017")
#     assert confirm_collection_size(source, "27017", collections, datasize)
#
# def test_initiate_pml():
#     result = pml.check_output(
#         "percona-mongolink start")
#     output = json.loads(result)
#     assert output in [{"ok": True}, {'error': 'already running', 'ok': False}]
#
# def test_data_transfer():
#     assert confirm_collection_size(destination, "27017", collections, datasize)
#
# def test_data_integrity():
#     assert compare_data_rs(source, destination, "27017")

def test_collect_performance_info():
    pmlAddress = obtain_pml_address(pml)
    collect_cpu_useage(source, pmlAddress)
    collect_cpu_useage(destination, pmlAddress)
    assert 1 == 2
