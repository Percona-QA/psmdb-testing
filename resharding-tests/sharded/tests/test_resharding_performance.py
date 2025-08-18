import os
import testinfra
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
TIMEOUT = os.getenv("TIMEOUT")

def check_mongod_service(node):
    with node.sudo():
        service = node.service("mongod")
        assert service.is_running

def restart_mongod(node):
    test = 0
    with node.sudo():
        result = node.check_output('systemctl restart mongod')
    print('restarting mongod: ' + result)

def restart_mongos(node):
    with node.sudo():
        result = node.check_output('systemctl restart mongos')
    print('restarting mongos: ' + result)

def load_data(count):
    config = [{'database': 'test','collection': 'binary','count': 1,'shardConfig': {
        'shardCollection': 'test.binary', 'key': {'num': 'hashed' }}, 'content': {
            'num': { 'type': 'long', 'minLong': 0, 'maxLong': 9223372036854775807 },
            'str': { 'type': 'string', 'minLength': 20, 'maxLength': 20 },
            'binary': {'type': 'binary','minLength': 1048552, 'maxLength': 1048552}
            },
        'indexes': [
            {'name':'idx_1','key': {'str': 'hashed'}}
            ]
        }]
    config[0]["count"] = count
    config_json = json.dumps(config, indent=4)
    print(config_json)
    primary_cfg.run_test('echo \'' + config_json + '\' > /tmp/generated_config.json')
    primary_cfg.run_test('mgodatagen --uri=mongodb://127.0.0.1:27017/ -f /tmp/generated_config.json -b 10')

def append_data(timeout):
    config = [{'database': 'test','collection': 'binary','count': 1, 'content': {
        'num': { 'type': 'long', 'minLong': 0, 'maxLong': 9223372036854775807 },
        'str': { 'type': 'string', 'minLength': 20, 'maxLength': 20 },
        'binary': {'type': 'binary','minLength': 1000, 'maxLength': 1000}
        }}]
    config[0]["count"] = int(timeout)*1000
    config_json = json.dumps(config, indent=4)
    print(config_json)
    primary_cfg.run_test('echo \'' + config_json + '\' > /tmp/generated_config.json')
    primary_cfg.run_test('timeout -s 9 ' + timeout + ' mgodatagen --uri=mongodb://127.0.0.1:27017/ -f /tmp/generated_config.json -n 1 -a -b 100 >/tmp/append.txt 2>&1 &')

def collect_stats(node,port,timeout):
    node.run_test('timeout -s 9 ' + timeout + ' mongostat --port ' + port + ' >/tmp/mongostat.txt 2>&1 &')
    node.run_test('timeout -s 9 ' + timeout + ' top -u mongod -b -n ' + timeout + ' > /tmp/top.txt 2>&1 &')
    node.run_test('timeout -s 9 ' + timeout + ' iostat -z -t 1 ' + timeout + ' >/tmp/iostat.txt 2>&1 &')

def get_mongostat(node):
    mongostat = node.check_output('cat /tmp/mongostat.txt')
    print(mongostat)

def get_top(node):
    top = node.check_output('cat /tmp/top.txt')
    print(top)

def get_iostat(node):
    iostat = node.check_output('cat /tmp/iostat.txt')
    print(iostat)

def get_generator_result():
    gen = primary_cfg.check_output('cat /tmp/append.txt')
    print(gen)

def check_count_data():
    result = primary_cfg.check_output("mongo mongodb://127.0.0.1:27017/test --eval 'db.binary.countDocuments({})' --quiet | tail -1")
    print('count objects in collection: ' + result)
    return result

def check_sharded_status():
    result = primary_cfg.check_output("mongo mongodb://127.0.0.1:27017/test --eval 'sh.status()' --quiet")
    print('sh.status:')
    print(result)

def check_distribution_info():
    result = primary_cfg.check_output("mongo mongodb://127.0.0.1:27017/test --eval 'db.binary.getShardDistribution()' --quiet")
    print('db.binary.getShardDistribution:')
    print(result)

def reshard_collection():
    result = primary_cfg.check_output("mongo mongodb://127.0.0.1:27017/admin --eval 'db.runCommand({reshardCollection: \"test.binary\", key: {\"str\": \"hashed\"}})' --quiet")
    print('reshard collection binary:')
    print(result)

def test_1_prepare_base_data():
    load_data(SIZE)
    count = check_count_data()
    assert int(count) == SIZE
    check_sharded_status()
    check_distribution_info()

def test_2_load_and_reshard():
    append_data(TIMEOUT)
    collect_stats(primary_cfg,"27019",TIMEOUT)
    for i in [primary_rs0, primary_rs1]:
        collect_stats(i,"27018",TIMEOUT)
    time.sleep(240)
    now = datetime.utcnow()
    resharding_start = now.strftime("%Y-%m-%dT%H:%M:%S")
    print("resharding start time: " + resharding_start)
    reshard_collection()
    now = datetime.utcnow()
    resharding_finish = now.strftime("%Y-%m-%dT%H:%M:%S")
    print("resharding finish time: " + resharding_finish)
    time.sleep(240)
    check_sharded_status()
    check_distribution_info()

def test_3_get_top_configserver():
    print("primary configserver stats:")
    get_top(primary_cfg)

def test_3_get_mongostat_configserver():
    print("primary configserver stats:")
    get_mongostat(primary_cfg)

def test_3_get_iostat_configserver():
    print("primary configserver stats:")
    get_iostat(primary_cfg)

def test_4_get_top_primary_rs0():
    print("primary configserver stats:")
    get_top(primary_rs0)

def test_4_get_mongostat_primary_rs0():
    print("primary configserver stats:")
    get_mongostat(primary_rs0)

def test_4_get_iostat_primary_rs0():
    print("primary configserver stats:")
    get_iostat(primary_rs0)

def test_5_get_top_primary_rs1():
    print("primary configserver stats:")
    get_top(primary_rs1)

def test_5_get_mongostat_primary_rs1():
    print("primary configserver stats:")
    get_mongostat(primary_rs1)

def test_5_get_iostat_primary_rs1():
    print("primary configserver stats:")
    get_iostat(primary_rs1)
