import os
import re
import pytest

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

BINARIES = ['mongo', 'mongod', 'mongos', 'bsondump', 'mongoexport', 'mongobridge',
            'mongofiles', 'mongoimport', 'mongorestore', 'mongotop', 'mongostat']

#PSMDB_VER = os.environ.get("PSMDB_VERSION")
PSMDB_VER = re.search(r'mongodb-(\d+\.\d+\.\d+)',os.environ.get("PSMDB_VERSION")).group(1)

@pytest.mark.parametrize("binary", BINARIES)
def test_binary_version(host, binary):
    mongod_process = host.process.filter(comm='mongod')
    cur_dir = mongod_process[0].args.split(sep="mongod")[0]
    result = host.run(f"{cur_dir}{binary} --version")
    assert PSMDB_VER in result.stdout, result.stdout

def test_running_version(host):
    mongod_process = host.process.filter(comm='mongod')
    print()
    for i in mongod_process:
        cur_dir = i.args.split(sep="mongod")[0]
        cur_port = i.args.split(sep="--port ")[1].split(sep=' ')[0]
        print('check mongod version running on port',cur_port)
        result = host.run(f"{cur_dir}mongo --port {cur_port} --quiet --eval 'db.version()'")
        assert PSMDB_VER in result.stdout, result.stdout

def test_compatibility_version(host):
    mongod_process = host.process.filter(comm='mongod')
    COMP_VER = PSMDB_VER.split(sep='.')[0] + "." + PSMDB_VER.split(sep='.')[1]
    print()
    print('compatibility version has to be',COMP_VER)
    for i in mongod_process:
        cur_dir = i.args.split(sep="mongod")[0]
        cur_port = i.args.split(sep="--port ")[1].split(sep=' ')[0]
        print('check mongod compatibility version running on port',cur_port)
        mongo_command = "db.adminCommand( { getParameter: 1, featureCompatibilityVersion: 1 } )"
        result = host.run(f"{cur_dir}mongo --port {cur_port} --quiet --eval '{mongo_command}'")
        assert COMP_VER in result.stdout, result.stdout
