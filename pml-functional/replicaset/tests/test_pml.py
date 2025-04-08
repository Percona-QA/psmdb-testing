import os
import random

import pytest
import testinfra
import json
import time
import testinfra.utils.ansible_runner

source = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('jenkins-pml-source')

destination = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('jenkins-pml-destination')

pml = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('jenkins-pml-mongolink')

collections = int(os.getenv("COLLECTIONS", default = 5))
datasize = int(os.getenv("DATASIZE", default = 100))
documentCount = int(datasize / 10 / collections)
distribute = os.getenv("DISTRIBUTE", default = "false")
TIMEOUT = int(os.getenv("TIMEOUT",default = 300))

def create_config(documentCount, collections):
    string = []
    for x in range(collections):
        collectionName = f"collection{x}"
        string2 = {'database': 'test','collection': collectionName,'count': documentCount,'content': {'binary': {'type': 'binary','minLength': 10485760, 'maxLength': 10485760}}}
        string.append(string2)
    return string

def distribute_create_config(dataSize, collections):
    string = []
    distribution_chunks = split_datasize(collections)

    for x in range(collections):
        distribution = dataSize / 100 * distribution_chunks[x]
        print("KEITH TEST SPLIT: " + str(int(distribution)))
        collectionName = f"collection{x}"
        string2 = {'database': 'test','collection': collectionName,'count': distribution,'content': {'binary': {'type': 'binary','minLength': 1048576, 'maxLength': 1048576}}}
        string.append(string2)
    return string

def split_datasize(chunks):
    parts = []
    remaining = 100
    for x in range(chunks - 1):
        max_number = remaining - (chunks - len(parts) - 1)
        n = random.randint(1, max_number)
        parts.append(n)
        remaining -= n
    parts.append(remaining)
    return parts

def pytest_configure():
    pytest.backup_name = ''
    pytest.pitr_start = ''
    pytest.pitr_end = ''

def find_event_msg(node,port,event,msg):
    command = "pbm logs --mongodb-uri=mongodb://localhost:" + port + "/ --tail=100 --out=json --event=" + event
    logs = node.check_output(command)
    for log in json.loads(logs):
        if log['msg'] == msg:
             return log
             break

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
        config = create_config(documentCount, collections)
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

def setup_pitr(node,port):
    result = node.check_output('pbm config --mongodb-uri=mongodb://localhost:' + port + '/ --set pitr.enabled=true --out=json')
    store_out = json.loads(result)
    print(store_out)

def test_3_prepare_data():
    # print(source)
    load_data(source,"27017")
    count = check_count_data(source,"27017")
    assert 1 == 1

# def test_1_print():
#     print("\nThe infrastructure is ready, waiting " + str(TIMEOUT) + " seconds")
# 
# def test_2_sleep():
#     time.sleep(TIMEOUT)
