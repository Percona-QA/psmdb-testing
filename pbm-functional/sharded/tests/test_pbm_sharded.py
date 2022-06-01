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

primary_cfg = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('primary-cfg')

secondary1_cfg = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary1-cfg')

secondary2_cfg = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary2-cfg')

primary_rs0 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('primary-rs0')

secondary1_rs0 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary1-rs0')

secondary2_rs0 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary2-rs0')

primary_rs1 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('primary-rs1')

secondary1_rs1 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary1-rs1')

secondary2_rs1 = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary2-rs1')

SIZE = int(os.getenv("SIZE"))
TIMEOUT = int(os.getenv("TIMEOUT"))
STORAGE = os.getenv("STORAGE")
BACKUP_TYPE = os.getenv("BACKUP_TYPE")

def pytest_configure():
    pytest.backup_name = ''
    pytest.pitr_start = ''
    pytest.pitr_end = ''

def find_backup(node,port,name):
    list = node.check_output('pbm list --mongodb-uri=mongodb://localhost:' + port + '/ --out=json')
    parsed_list = json.loads(list)
    if parsed_list['snapshots']:
        for snapshot in parsed_list['snapshots']:
            if snapshot['name'] == name:
                return snapshot
                break

def find_event_msg(node,port,event,msg):
    command = "pbm logs --mongodb-uri=mongodb://localhost:" + port + "/ --tail=100 --out=json --event=" + event
    logs = node.check_output(command)
    for log in json.loads(logs):
        if log['msg'] == msg:
             return log
             break

def get_pbm_logs(node,port):
    command = "pbm logs -s D --mongodb-uri=mongodb://localhost:" + port + "/ --tail=0"
    logs = node.check_output(command)
    print(logs)

def check_status(node,port):
    status = node.check_output('pbm status --mongodb-uri=mongodb://localhost:' + port + '/ --out=json 2>/dev/null')
    running = json.loads(status)['running']
    if running:
        return running

def check_pitr(node,port):
    status = node.check_output('pbm status --mongodb-uri=mongodb://localhost:' + port + '/ --out=json 2>/dev/null')
    running = json.loads(status)['pitr']['run']
    return bool(running)

def check_agents_status(node,port):
    result = node.check_output('pbm status --mongodb-uri=mongodb://localhost:' + port + '/ --out=json 2>/dev/null')
    parsed_result = json.loads(result)
    for replicaset in parsed_result['cluster']:
        for host in replicaset['nodes']:
            assert host['ok'] == True

def check_mongod_service(node):
    with node.sudo():
        service = node.service("mongod")
        assert service.is_running

def check_pbm_service(node):
    with node.sudo():
        service = node.service("pbm-agent")
        assert service.is_running

def restart_mongod(node):
    with node.sudo():
        result = node.check_output('systemctl restart mongod')
    print('restarting mongod: ' + result)

def restart_mongos(node):
    with node.sudo():
        result = node.check_output('systemctl restart mongos')
    print('restarting mongos: ' + result)

def restart_pbm_agent(node):
    with node.sudo():
        result = node.check_output('systemctl restart pbm-agent')
    print('restarting pbm-agent: ' + result)

def make_backup(node,port,type):
    for i in range(TIMEOUT):
        running = check_status(node,port)
        if not running:
            if type == 'physical':
                start = node.check_output('pbm backup --mongodb-uri=mongodb://localhost:' + port + '/ --out=json --type=' + type )
            else:
                start = node.check_output('pbm backup --mongodb-uri=mongodb://localhost:' + port + '/ --out=json')
            name = json.loads(start)['name']
            print("backup started:")
            print(name)
            break
        else:
            print("unable to start backup - another operation in work")
            print(running)
            time.sleep(1)
    for i in range(TIMEOUT):
        running = check_status(node,port)
        print("current operation:")
        print(running)
        result = find_backup(node,port,name)
        if result:
            print("backup found:")
            print(result)
            assert result['status'] == 'done'
            return name
            break
        else:
            time.sleep(1)

def make_restore(node,port,name):
    for i in range(TIMEOUT):
        running = check_status(node,port)
        if not running:
            output = node.check_output('pbm restore --mongodb-uri=mongodb://localhost:' + port + '/ ' + name + ' --wait')
            print(output)
            break
        else:
            print("unable to start restore - another operation in work")
            print(running)
            time.sleep(1)
    for i in [secondary1_cfg, secondary2_cfg, primary_cfg, secondary1_rs0, secondary2_rs0, primary_rs0, secondary1_rs1, secondary2_rs1, primary_rs1]:
        restart_mongod(i)
        time.sleep(5)
    for i in [secondary1_cfg, secondary2_cfg, primary_cfg, secondary1_rs0, secondary2_rs0, primary_rs0, secondary1_rs1, secondary2_rs1, primary_rs1]:
        restart_pbm_agent(i)
        time.sleep(5)
    for i in [secondary1_cfg, secondary2_cfg, primary_cfg]:
        restart_mongos(i)
        time.sleep(5)
    output = node.check_output('pbm config --mongodb-uri=mongodb://localhost:' + port + '/ --force-resync')
    print(output)
    for i in range(TIMEOUT):
        logs = find_event_msg(node,port,"resync","succeed")
        if logs:
            print(logs)
            break
        else:
            time.sleep(1)
    time.sleep(300)
    for i in [secondary1_cfg, secondary2_cfg, primary_cfg, secondary1_rs0, secondary2_rs0, primary_rs0, secondary1_rs1, secondary2_rs1, primary_rs1]:
        check_mongod_service(i)

def make_pitr_restore(node,port,name,timestamp):
    for i in range(TIMEOUT):
        running = check_status(node,port)
        if not running:
            output = node.check_output('pbm restore --mongodb-uri=mongodb://localhost:' + port + '/ --time=' + timestamp + ' --base-snapshot=' + name + ' --wait')
            print(output)
            break
        else:
            print("unable to start restore - another operation in work")
            print(running)
            time.sleep(1)
    for i in [secondary1_cfg, secondary2_cfg, primary_cfg, secondary1_rs0, secondary2_rs0, primary_rs0, secondary1_rs1, secondary2_rs1, primary_rs1]:
        restart_mongod(i)
        time.sleep(5)
    for i in [secondary1_cfg, secondary2_cfg, primary_cfg, secondary1_rs0, secondary2_rs0, primary_rs0, secondary1_rs1, secondary2_rs1, primary_rs1]:
        restart_pbm_agent(i)
        time.sleep(5)
    for i in [secondary1_cfg, secondary2_cfg, primary_cfg]:
        restart_mongos(i)
        time.sleep(5)

def make_pitr_replay(node,port,start,end):
    for i in range(TIMEOUT):
        running = check_status(node,port)
        if not running:
            output = node.check_output('pbm oplog-replay --mongodb-uri=mongodb://localhost:' + port + '/ --start=' + start + ' --end=' + end + ' --wait')
            print(output)
            break
        else:
            print("unable to start restore - another operation in work")
            print(running)
            time.sleep(1)

def load_data(node,port,count):
    config = [{'database': 'test','collection': 'binary','count': 1,'shardConfig': {'shardCollection': 'test.binary', 'key': {'_id': 'hashed'}}, 'content': {'binary': {'type': 'binary','minLength': 1048576, 'maxLength': 1048576}}}]
    config[0]["count"] = count
    config_json = json.dumps(config, indent=4)
    print(config_json)
    node.run_test('echo \'' + config_json + '\' > /tmp/generated_config.json')
    result = node.check_output('mgodatagen --uri=mongodb://127.0.0.1:' + port + '/ -f /tmp/generated_config.json --batchsize 10')

def check_count_data(node,port):
    result = node.check_output("mongo mongodb://127.0.0.1:" + port + "/test --eval 'db.binary.countDocuments({})' --quiet | tail -1")
    print('count objects in collection: ' + result)
    return result

def drop_database(node,port):
    result = node.check_output("mongo mongodb://127.0.0.1:" + port + "/test --eval 'db.dropDatabase()' --quiet")
    print(result)

def setup_pitr(node,port):
    result = node.check_output('pbm config --mongodb-uri=mongodb://localhost:' + port + '/ --set pitr.enabled=true --out=json')
    store_out = json.loads(result)
    print(store_out)

def test_1_setup_storage():
    result = primary_cfg.check_output('pbm config --mongodb-uri=mongodb://localhost:27019/ --file=/etc/pbm-agent-storage-' + STORAGE + '.conf --out=json')
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

def test_2_agents_status():
    check_agents_status(primary_cfg,"27019")

def test_3_prepare_data():
    load_data(primary_cfg,"27017",SIZE)
    count = check_count_data(primary_cfg,"27017")
    assert int(count) == SIZE

def test_4_setup_pitr():
    if BACKUP_TYPE == "physical":
        result = primary_cfg.check_output('pbm config --mongodb-uri=mongodb://localhost:27019/ --set pitr.enabled=true --set pitr.oplogOnly=true --out=json')
        for i in range(TIMEOUT):
            pitr = check_pitr(primary_cfg,"27019")
            if not pitr:
                print("waiting for pitr to be enabled") 
                time.sleep(1)
            else:
                print("pitr enabled")
                break
        assert check_pitr(primary_cfg,"27019") == True
    else:
        result = primary_cfg.check_output('pbm config --mongodb-uri=mongodb://localhost:27019/ --set pitr.enabled=true --out=json')
    store_out = json.loads(result)
    print(store_out)

def test_5_backup():
    now = datetime.utcnow()
    pytest.pitr_start = now.strftime("%Y-%m-%dT%H:%M:%S")
    print("pitr start time: " + pytest.pitr_start)
    pytest.backup_name = make_backup(primary_cfg,"27019",BACKUP_TYPE)
    for i in range(TIMEOUT):
        pitr = check_pitr(primary_cfg,"27019")
        if not pitr:
            print("waiting for pitr to be enabled")
            time.sleep(1)
        else:
            print("pitr enabled")
            break
    assert check_pitr(primary_cfg,"27019") == True
    print("pbm logs:")
    get_pbm_logs(primary_cfg,"27019")

def test_6_modify_data():
    drop_database(primary_cfg,"27017")
    load_data(primary_cfg,"27017",10)
    count = check_count_data(primary_cfg,"27017")
    assert int(count) == 10
    time.sleep(60)
    now = datetime.utcnow()
    pytest.pitr_end = now.strftime("%Y-%m-%dT%H:%M:%S")
    print("pitr end time: " + pytest.pitr_end)

def test_7_disable_pitr():
    result = primary_cfg.check_output('pbm config --mongodb-uri=mongodb://localhost:27019/ --set pitr.enabled=false --out=json')
    store_out = json.loads(result)
    print(store_out)
    for i in range(TIMEOUT):
        pitr = check_pitr(primary_cfg,"27019")
        if pitr:
            time.sleep(1)
            print("waiting for pitr to be disabled")
        else:
            print("pitr disabled")
            break
    assert check_pitr(primary_cfg,"27019") == False

def test_8_restore():
    make_restore(primary_cfg,"27019",pytest.backup_name)
    count = check_count_data(primary_cfg,"27017")
    assert int(count) == SIZE
    print("pbm logs:")
    get_pbm_logs(primary_cfg,"27019")

def test_9_pitr_restore():
    if BACKUP_TYPE == "logical":
        print("performing pitr restore from backup " + pytest.backup_name + " to timestamp " + pytest.pitr_end)
        make_pitr_restore(primary_cfg,"27019",pytest.backup_name,pytest.pitr_end)
        count = check_count_data(primary_cfg,"27017")
        assert int(count) == 10
    if BACKUP_TYPE == "physical":
        print("performing pitr replay from  " + pytest.pitr_start + " to " + pytest.pitr_end)
        make_pitr_replay(primary_cfg,"27019",pytest.pitr_start,pytest.pitr_end)
        count = check_count_data(primary_cfg,"27017")
        assert int(count) == 10
    print("pbm logs:")
    get_pbm_logs(primary_cfg,"27019")

