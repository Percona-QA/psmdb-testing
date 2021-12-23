import os
import pytest
import testinfra
import subprocess
import json
import time
import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('mongo')

primary_rs0 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('primary-rs0')

secondary1_rs0 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary1-rs0')

secondary2_rs0 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary2-rs0')

SIZE = int(os.getenv("SIZE"))
TIMEOUT = int(os.getenv("TIMEOUT"))
STORAGE = os.getenv("STORAGE")
BACKUP_TYPE = os.getenv("BACKUP_TYPE")

def pytest_configure():
    pytest.backup_name = ''

def find_backup(node,name):
    list = node.check_output('pbm list --mongodb-uri=mongodb://localhost:27017/ --out=json')
    parsed_list = json.loads(list)
    if parsed_list['snapshots']:
        for snapshot in parsed_list['snapshots']:
            if snapshot['name'] == name:
                return snapshot
                break

def find_event_msg(node,event,msg):
    if event:
        command = "pbm logs --mongodb-uri=mongodb://localhost:27017/ --tail=0 --out=json --event=" + event
    else:
        command = "pbm logs --mongodb-uri=mongodb://localhost:27017/ --tail=0 --out=json"
    logs = node.check_output(command) 
    for log in json.loads(logs):
        if log['msg'] == msg:
             return log
             break

def check_status(node):
    status = node.check_output('pbm status --mongodb-uri=mongodb://localhost:27017/ --out=json')
    running = json.loads(status)['running']
    if running:
        return running

def restart_mongod(node):
    with node.sudo():
        result = node.check_output('systemctl restart mongod')
    print('restarting mongod: ' + result)

def restart_pbm_agent(node):
    with node.sudo():
        result = node.check_output('systemctl restart pbm-agent')
    print('restarting pbm-agent: ' + result)

def make_backup(node,type):
    for i in range(TIMEOUT):
        running = check_status(node)
        if not running:
            if type:
                start = node.check_output('pbm backup --mongodb-uri=mongodb://localhost:27017/ --out=json --type=' + type )
            else:
                start = node.check_output('pbm backup --mongodb-uri=mongodb://localhost:27017/ --out=json')
            name = json.loads(start)['name']
            print("backup started:")
            print(name)
            break
        else:
            print("unable to start backup - another operation in work")
            print(running)
            time.sleep(1)
    for i in range(TIMEOUT):
        running = check_status(node)
        print("current operation:")
        print(running)
        result = find_backup(node,name)
        if result:
            print("backup found:")
            print(result)
            assert result['status'] == 'done'
            return name
            break
        else:
            time.sleep(1)

def make_restore(node,name):
    for i in range(TIMEOUT):
        running = check_status(node)
        if not running:
            output = node.check_output('pbm restore --mongodb-uri=mongodb://localhost:27017/ ' + name + ' --wait')
            print(output)
            break
        else:
            print("unable to start restore - another operation in work")
            print(running)
            time.sleep(1)
    for i in [secondary1_rs0, secondary2_rs0, primary_rs0]:
        restart_mongod(i)
        time.sleep(5)
    time.sleep(5)
    for i in [secondary1_rs0, secondary2_rs0, primary_rs0]:
        restart_pbm_agent(i)
        time.sleep(5)
    time.sleep(5)

def load_data(node,count):
    config = [{'database': 'test','collection': 'binary','count': 1,'content': {'binary': {'type': 'binary','minLength': 1048576, 'maxLength': 1048576}}}]
    config[0]["count"] = count
    config_json = json.dumps(config, indent=4)
    print(config_json)
    node.run_test('echo \'' + config_json + '\' > /tmp/generated_config.json')
    result = node.check_output('mgodatagen --uri=mongodb://127.0.0.1:27017/?replicaSet=rs0 -f /tmp/generated_config.json --batchsize 10')

def check_count_data(node):
    result = node.check_output("mongo mongodb://127.0.0.1:27017/test?replicaSet=rs0 --eval 'db.binary.count()' --quiet | tail -1")
    print('count objects in collection: ' + result)
    return result

def drop_database(node):
    result = node.check_output("mongo mongodb://127.0.0.1:27017/test?replicaSet=rs0 --eval 'db.dropDatabase()' --quiet")
    print(result)

def test_setup_storage():
    result = primary_rs0.check_output('pbm config --mongodb-uri=mongodb://localhost:27017/ --file=/etc/pbm-agent-storage-' + STORAGE + '.conf --out=json')
    store_out = json.loads(result)
    if STORAGE == "minio":
        assert store_out['storage']['type'] == 's3'
        assert store_out['storage']['s3']['region'] == 'us-east-1'
        assert store_out['storage']['s3']['endpointUrl'] == 'http://minio:9000'
    if STORAGE == "aws":
        assert store_out['storage']['type'] == 's3'
        assert store_out['storage']['s3']['region'] == 'us-east-2'
        assert store_out['storage']['s3']['bucket'] == 'pbm-testing' 
    time.sleep(10)

def test_agent_status_before(host):
    result = host.check_output('pbm status --mongodb-uri=mongodb://localhost:27017/ --out=json')
    parsed_result = json.loads(result)
    for replicaset in parsed_result['cluster']:
        for host in replicaset['nodes']:
            assert host['ok'] == True

def test_prepare_data():
    load_data(primary_rs0,SIZE)
    count = check_count_data(primary_rs0)
    assert int(count) == SIZE

def test_backup():
    pytest.backup_name = make_backup(primary_rs0,BACKUP_TYPE)

def test_drop_data():
    drop_database(primary_rs0)
    count = check_count_data(primary_rs0)
    assert int(count) == 0

def test_restore():
    make_logical_restore(secondary1_rs0,pytest.backup_name)
    count = check_count_data(primary_rs0)
    assert int(count) == SIZE

def test_agent_status_after(host):
    result = host.check_output('pbm status --mongodb-uri=mongodb://localhost:27017/ --out=json')
    parsed_result = json.loads(result)
    for replicaset in parsed_result['cluster']:
        for host in replicaset['nodes']:
            assert host['ok'] == True
