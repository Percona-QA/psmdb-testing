import os
import pytest
import testinfra
import subprocess
import json
import time
import testinfra.utils.ansible_runner
from datetime import datetime

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('mongo')

primary = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('primary-rs-initsync')

secondary1 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary1-rs-initsync')

secondary2 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary2-rs-initsync')

DOC_SIZE = int(os.getenv("DOC_SIZE",1024))
IDX_COUNT = int(os.getenv("IDX_COUNT",2))
DOC_COUNT = int(os.getenv("DOC_COUNT",1000 * 1000))
DB_COUNT = int(os.getenv("DB_COUNT",10))
TIMEOUT = int(os.getenv("TIMEOUT",3600))

def check_mongod_service(node):
    with node.sudo():
        service = node.service("mongod")
        assert service.is_running

def restart_mongod(node):
    with node.sudo():
        result = node.check_output('systemctl restart mongod')
    print('restarting mongod: ' + result)

def stop_mongod(node):
    with node.sudo():
        result = node.check_output('systemctl stop mongod')
    print('Stop mongod')

def start_mongod(node):
    with node.sudo():
        result = node.check_output('systemctl start mongod')
    print('Start mongod')

def cleanup_data(node):
    with node.sudo():
        node.check_output('rm -rf /mnt/data/db/*')
        node.check_output('truncate -s0 /var/log/mongo/mongod.log')
    print('The data and logs are erased')

def wait_secondary(node):
    timeout = time.time() + TIMEOUT
    while True:
        check_mongod_service(node)
        state = node.run("timeout 3 mongosh --quiet --eval 'db.hello().secondary'")
        if 'true' in state.stdout:
            print('The node is in secondary state')
            break
        if time.time() > timeout:
            assert False
        time.sleep(1)

def wait_init_sync(node):
    with node.sudo():
        timeout = time.time() + TIMEOUT
        while True:
            log = node.run('grep "Initial sync status and statistics" /var/log/mongo/mongod.log')
            if 'status' in log.stdout:
                print(log.stdout)
                break
            if time.time() > timeout:
                assert False
            time.sleep(1)

def get_db_size(node):
    with node.sudo():
        size = node.check_output('du -sh /mnt/data/db')
        print(size)

def load_data(doc_size,idx_count,doc_count,db_count):
    config = []
    for i in range(db_count):
        db_name = 'test' + str(i)
        db = {'database': db_name,'collection': 'test','count': doc_count, 'content': {}, 'indexes': []}
        db['content']['data'] = {'type': 'binary','minLength': doc_size, 'maxLength': doc_size }
        for j in range(idx_count):
              obj = 'doc' + str(j)
              db['content'][obj] = {'type': 'string','minLength': 8, 'maxLength': 8 }
              idx_name = 'idx_' + str(j)
              idx = {'name': idx_name,'key':{ obj: 1}}
              db['indexes'].append(idx)
        config.append(db)
    config_json = json.dumps(config, indent=4)
    print(config_json)
    primary.run_test('echo \'' + config_json + '\' > /tmp/generated_config.json')
    primary.run_test('mgodatagen --uri=mongodb://127.0.0.1:27017/ -f /tmp/generated_config.json')

def test_1_prepare_base_data():
    print('\nLoad base data')
    load_data(DOC_SIZE,IDX_COUNT,DOC_COUNT,DB_COUNT)

def test_2_get_db_size():
    print('\nGet data size')
    get_db_size(secondary1)

def test_3_cleanup_secondary():
    print('\nCleanup node')
    stop_mongod(secondary2)
    cleanup_data(secondary2)

def test_4_initsync():
    print('\nStart initial sync')
    start_mongod(secondary2)
    wait_secondary(secondary2)
    wait_init_sync(secondary2)
