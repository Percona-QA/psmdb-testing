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

def find_backup(node,name):
    list = node.check_output('pbm list --out=json')
    parsed_list = json.loads(list)
    if parsed_list['snapshots']:
        for snapshot in parsed_list['snapshots']:
            if snapshot['name'] == name:
                return snapshot
                break

def find_event_msg(node,event,msg):
    if event:
        command = "pbm logs --tail=0 --out=json --event=" + event
    else:
        command = "pbm logs --tail=0 --out=json"
    logs = node.check_output(command) 
    for log in json.loads(logs):
        if log['msg'] == msg:
             return log
             break

def check_status(node):
    status = node.check_output('pbm status --out=json')
    running = json.loads(status)['running']
    if running:
        return running

def make_backup(node,type):
    for i in range(30):
        running = check_status(node)
        if not running:
            if type:
                start = node.check_output('pbm backup --out=json --type='+type )
            else:
                start = node.check_output('pbm backup --out=json')
            name = json.loads(start)['name']
            print("backup started:")
            print(name)
            break
        else:
            print("unable to start backup - another operation in work")
            print(running)
            time.sleep(1)
    for i in range(30):
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

def make_logical_restore(node,name):
    for i in range(30):
        running = check_status(node)
        if not running:
            output = node.check_output('pbm restore ' + name)
            print(output)
            break
        else:
            print("unable to start restore - another operation in work")
            print(running)
            time.sleep(1)
    for i in range(30):
        running = check_status(node)
        print("current operation:")
        print(running)
        result = find_event_msg(node,"restore/" + name,"restore finished successfully")
        if result:
            print(result)
            break
        else:
            time.sleep(1)

def make_physical_restore(node,name):
    for i in range(30):
        running = check_status(node)
        if not running:
            output = node.check_output('pbm restore ' + name + ' --wait')
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

def restart_mongod(node):
    result = node.check_output('systemctl restart mongod')
    print('restarting mongod: ' + result)

def restart_pbm_agent(node):
    result = node.check_output('systemctl restart pbm-agent')
    print('restarting pbm-agent: ' + result)

def load_data(node):
    #ideally - generate own mgodatagen config
    result = node.run('mgodatagen --uri=mongodb://127.0.0.1:27017/?replicaSet=rs0 -f /tmp/index_basic.json')

def check_count_data(node):
    result = node.check_output("mongo mongodb://127.0.0.1:27017/mgodatagen_test?replicaSet=rs0 --eval 'db.index_basic.count()' --quiet | tail -1")
    print('count objects in collection: ' + result)
    return result

def drop_database(node):
    result = node.check_output("mongo mongodb://127.0.0.1:27017/mgodatagen_test?replicaSet=rs0 --eval 'db.dropDatabase()' --quiet")
    print(result)

def test_setup_minio():
    result = primary_rs0.check_output('pbm config --file=/etc/pbm-agent-storage-minio.conf --out=json')
    store_out = json.loads(result)
    assert store_out['storage']['type'] == 's3'
    assert store_out['storage']['s3']['region'] == 'us-east-1'
    assert store_out['storage']['s3']['endpointUrl'] == 'http://minio:9000'
    time.sleep(5)

def test_agent_status(host):
    result = host.run('pbm status --out=json')
    parsed_result = json.loads(result.stdout)
    for replicaset in parsed_result['cluster']:
        for host in replicaset['nodes']:
            assert host['ok'] == True

def test_backup_logical_restore_minio():
    drop_database(primary_rs0)
    load_data(primary_rs0)
    count = check_count_data(primary_rs0)
    backup_name = make_backup(primary_rs0,'logical')
    drop_database(primary_rs0)
    make_logical_restore(secondary1_rs0,backup_name)
    new_count = check_count_data(primary_rs0)
    assert count == new_count

def test_backup_physical_restore_minio():
    drop_database(primary_rs0)
    load_data(primary_rs0)
    count = check_count_data(primary_rs0)
    backup_name = make_backup(primary_rs0,'physical')
    drop_database(primary_rs0)
    make_physical_restore(secondary1_rs0,backup_name)
    new_count = check_count_data(primary_rs0)
    assert count == new_count
