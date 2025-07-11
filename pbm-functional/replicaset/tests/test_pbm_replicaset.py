import logging
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

primary_rs = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('primary-rs')

secondary1_rs = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary1-rs')

secondary2_rs = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('secondary2-rs')

SIZE = int(os.getenv("SIZE",default = 10))
TIMEOUT = int(os.getenv("TIMEOUT",default = 300))
STORAGE = os.getenv("STORAGE")
BACKUP_TYPE = os.getenv("BACKUP_TYPE")
EXISTING_BACKUP = os.getenv("EXISTING_BACKUP",default = "no")
CHECK_PITR = os.getenv("CHECK_PITR",default = "yes")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE",default = "16777216"))
numDownloadWorkers = os.getenv("RESTORE_NUMDOWNLOADWORKERS",default = '0')
maxDownloadBufferMb = os.getenv("RESTORE_NUMDOWNLOADBUFFERMB",default = '0')
downloadChunkMb = os.getenv("RESTORE_DOWNLOADCHUNKMB",default = '0')

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
    print("KEITH TEST 1 :" + str(json.loads(status)))
    print("KEITH TEST 2 :" + str(running))
    return bool(running)

def check_agents_status(node,port):
    result = node.check_output('pbm status --mongodb-uri=mongodb://localhost:' + port + '/ --out=json')
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
        hostname = node.check_output('hostname -s')
        result = node.check_output('systemctl restart mongod')
    print('restarting mongod on ' + hostname)

def restart_pbm_agent(node):
    with node.sudo():
        hostname = node.check_output('hostname -s')
        result = node.check_output('systemctl restart pbm-agent')
    print('restarting pbm-agent on ' + hostname)

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
    for i in [secondary1_rs, secondary2_rs, primary_rs]:
        restart_mongod(i)
        time.sleep(5)
    time.sleep(5)
    for i in [secondary1_rs, secondary2_rs, primary_rs]:
        restart_pbm_agent(i)
        time.sleep(5)
    for i in [secondary1_rs, secondary2_rs, primary_rs]:
        with i.sudo():
            hostname = i.check_output('hostname -s')
            logs = i.check_output('journalctl -u pbm-agent -b -o cat --no-pager | grep restore | grep download || true')
            print("logs from " + hostname)
            print(logs)
    output = node.check_output('pbm config --mongodb-uri=mongodb://localhost:' + port + '/ --force-resync')
    print(output)
    for i in range(TIMEOUT):
        logs = find_event_msg(node,"27017","resync","succeed")
        if logs:
            print(logs)
            break
        else:
            time.sleep(1)
    for i in [secondary1_rs, secondary2_rs, primary_rs]:
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
    for i in [secondary1_rs, secondary2_rs, primary_rs]:
        restart_mongod(i)
        time.sleep(5)
    time.sleep(5)
    for i in [secondary1_rs, secondary2_rs, primary_rs]:
        restart_pbm_agent(i)
        time.sleep(5)
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
    config = [{'database': 'test','collection': 'binary','count': 1,'content': {'binary': {'type': 'binary','minLength': 1048576, 'maxLength': 1048576}}}]
    config[0]["count"] = count
    config_json = json.dumps(config, indent=4)
    print(config_json)
    node.run_test('echo \'' + config_json + '\' > /tmp/generated_config.json')
    result = node.check_output('mgodatagen --uri=mongodb://127.0.0.1:' + port + '/?replicaSet=rs -f /tmp/generated_config.json --batchsize 10')

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

def test_1_setup_storage():
    primary_rs.check_output(f'pbm config --mongodb-uri=mongodb://localhost:27017/ --file=/etc/pbm-agent-storage-{STORAGE}.conf --out=json')
    if STORAGE == "gcp-hmac" or STORAGE == "gcp":
        primary_rs.check_output(
            f"pbm config --mongodb-uri=mongodb://localhost:27017/ --set storage.gcs.chunkSize={CHUNK_SIZE} --set storage.gcs.prefix=pbm/test --out=json"
        )
    store_out = json.loads(primary_rs.check_output("pbm config --mongodb-uri=mongodb://localhost:27017/ --list --out=json"))
    print("KEITH TEST " + str(store_out))
    if STORAGE == "minio":
        assert store_out['storage']['type'] == 's3'
        assert store_out['storage']['s3']['region'] == 'us-east-1'
        assert store_out['storage']['s3']['endpointUrl'] == 'http://minio:9000'
    if STORAGE == "aws":
        assert store_out['storage']['type'] == 's3'
        assert store_out['storage']['s3']['region'] == 'us-west-2'
        assert store_out['storage']['s3']['bucket'] == 'pbm-testing-west'
    if STORAGE == "gcp-hmac" or STORAGE == "gcp":
        assert store_out['storage']['type'] == 'gcs'
        assert store_out['storage']['gcs']['chunkSize'] == CHUNK_SIZE
        assert store_out['storage']['gcs']['prefix'] == 'pbm/test'
    d = {'numDownloadWorkers': numDownloadWorkers,'maxDownloadBufferMb': maxDownloadBufferMb,'downloadChunkMb': downloadChunkMb }
    for k, v in d.items():
        if int(v):
            result = primary_rs.check_output('pbm config --mongodb-uri=mongodb://localhost:27017/ --set restore.' + k + '=' + v + ' --out=json')
            store_out = json.loads(result)
            print(store_out)
    time.sleep(10)

def test_2_agents_status():
    check_agents_status(primary_rs,"27017")

def test_3_prepare_data():
    if EXISTING_BACKUP != "no":
        pytest.skip("Skipping loading data")
    load_data(primary_rs,"27017",SIZE)
    count = check_count_data(primary_rs,"27017")
    assert int(count) == SIZE

def test_4_setup_pitr():
    if EXISTING_BACKUP != "no" or CHECK_PITR == "no":
        pytest.skip("Skipping pitr test")
    if BACKUP_TYPE == "physical":
        pytest.backup_name = make_backup(primary_rs, "27017", BACKUP_TYPE)
        result = primary_rs.check_output('pbm config --mongodb-uri=mongodb://localhost:27017/ --set pitr.enabled=true --set pitr.oplogOnly=true --out=json')
        for i in range(TIMEOUT):
            pitr = check_pitr(primary_rs,"27017")
            if not pitr:
                print("waiting for pitr to be enabled")
                time.sleep(1)
            else:
                print("pitr enabled")
                break
        assert check_pitr(primary_rs,"27017") == True
    else:
        result = primary_rs.check_output('pbm config --mongodb-uri=mongodb://localhost:27017/ --set pitr.enabled=true --out=json')
    store_out = json.loads(result)
    print(store_out)

def test_5_backup():
    if EXISTING_BACKUP != "no":
        pytest.skip("Skipping backup test")
    now = datetime.utcnow()
    pytest.pitr_start = now.strftime("%Y-%m-%dT%H:%M:%S")
    print("pitr start time: " + pytest.pitr_start)
    pytest.backup_name = make_backup(primary_rs,"27017",BACKUP_TYPE)
    if CHECK_PITR != "no":
        for i in range(TIMEOUT):
            pitr = check_pitr(primary_rs,"27017")
            if not pitr:
                print("waiting for pitr to be enabled")
                time.sleep(1)
            else:
                print("pitr enabled")
                break
        assert check_pitr(primary_rs,"27017") == True

def test_6_modify_data():
    if EXISTING_BACKUP != "no":
        pytest.skip("Skipping backup test")
    drop_database(primary_rs,"27017")
    load_data(primary_rs,"27017",10)
    count = check_count_data(primary_rs,"27017")
    assert int(count) == 10
    time.sleep(60)
    now = datetime.utcnow()
    pytest.pitr_end = now.strftime("%Y-%m-%dT%H:%M:%S")
    print("pitr end time: " + pytest.pitr_end)

def test_7_disable_pitr():
    if EXISTING_BACKUP != "no" or CHECK_PITR == "no":
        pytest.skip("Skipping pitr test")
    result = primary_rs.check_output('pbm config --mongodb-uri=mongodb://localhost:27017/ --set pitr.enabled=false --out=json')
    store_out = json.loads(result)
    print(store_out)
    time.sleep(60)
    for i in range(TIMEOUT):
        pitr = check_pitr(primary_rs,"27017")
        if pitr:
            time.sleep(1)
            print("waiting for pitr to be disabled")
        else:
            print("pitr disabled")
            break
    assert check_pitr(primary_rs,"27017") == False

def test_8_restore():
    if EXISTING_BACKUP != "no":
        pytest.backup_name = EXISTING_BACKUP
    make_restore(secondary1_rs,"27017",pytest.backup_name)
    count = check_count_data(primary_rs,"27017")
    assert int(count) == SIZE
#
# # def test_10_wait():
# #     time.sleep(3600)
#
def test_9_pitr_restore():
    if EXISTING_BACKUP != "no" or CHECK_PITR == "no":
        pytest.skip("Skipping pitr test")
    if BACKUP_TYPE == "logical":
        print("performing pitr restore from backup " + pytest.backup_name + " to timestamp " + pytest.pitr_end)
        make_pitr_restore(secondary1_rs,"27017",pytest.backup_name,pytest.pitr_end)
        count = check_count_data(primary_rs,"27017")
        assert int(count) == 10
    if BACKUP_TYPE == "physical":
        print("performing pitr replay from  " + pytest.pitr_start + " to " + pytest.pitr_end)
        make_pitr_restore(primary_rs,"27017",pytest.backup_name,pytest.pitr_end)
        count = check_count_data(primary_rs,"27017")
        assert int(count) == 10
