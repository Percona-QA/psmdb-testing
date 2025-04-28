import os
import random
import sys
from time import sleep
import json
import testinfra.utils.ansible_runner
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from mlink.data_integrity_check import compare_data_rs
from mlink.cluster import Cluster

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

def randomize_sizes(total_bytes, min_size=512 * 1024, max_size=16 * 1024 * 1024):
    sizes = []
    remaining = total_bytes

    while remaining > 0:
        max_allowed = min(max_size, remaining)
        size = random.randint(min_size, max_allowed)
        sizes.append(size)
        remaining -= size

    return sizes

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

def pml_start():
    try:
        output = json.loads(pml.check_output("curl -s -X POST http://localhost:2242/start -d '{}'"))

        if output:
            try:
                if output.get("ok") is True or output.get("error") == "already running":
                    Cluster.log("Sync started successfully")
                    return True

                elif output.get("ok") is False and output.get("error") != "already running":
                    error_msg = output.get("error", "Unknown error")
                    Cluster.log(f"Failed to start sync between src and dst cluster: {error_msg}")
                    return False

            except json.JSONDecodeError:
                Cluster.log("Received invalid JSON response.")

        Cluster.log("Failed to start sync between src and dst cluster")
        return False
    except Exception as e:
        Cluster.log(f"Unexpected error: {e}")
        return False


def pml_finalize():
    try:
        output = json.loads(pml.check_output("curl -s -X POST http://localhost:2242/finalize -d '{}'"))

        if output:
            try:

                if output.get("ok") is True:
                    Cluster.log("Sync finalized successfully")
                    return True

                elif output.get("ok") is False:
                    error_msg = output.get("error", "Unknown error")
                    Cluster.log(f"Failed to finalize sync between src and dst cluster: {error_msg}")
                    return False

            except json.JSONDecodeError:
                Cluster.log("Received invalid JSON response.")

        Cluster.log("Failed to finalize sync between src and dst cluster")
        return False
    except Exception as e:
        Cluster.log(f"Unexpected error: {e}")
        return False

# def pml_start(timeout=120):
#     result = json.loads(pml.check_output(
#         "percona-mongolink start"))
#     for _ in range(timeout):
#         status = json.loads(pml.check_output('percona-mongolink status'))
#         if status["initialSync"]["cloneCompleted"] == True:
#             return True
#         sleep(1)
#     print("PML did not start after " + str(timeout) + " seconds.")
#     return False

# def pml_finalize(timeout=120):
#     pml.check_output(
#         "percona-mongolink finalize")
#     for _ in range(timeout):
#         status = json.loads(pml.check_output('percona-mongolink status'))
#         if status["state"] == "finalized":
#             return True
#         sleep(1)
#     print("PML did not finalize after " + str(timeout) + " seconds.")
#     return False

# def test_prepare_data():
#     load_data(source,"27017")
#     assert confirm_collection_size(source, "27017", collections, datasize)

def test_initiate_pml():
    result = json.loads(pml.check_output(
        "percona-mongolink start"))
    assert result in [{"ok": True}, {'error': 'already running', 'ok': False}]
    assert pml_start()
    assert pml_finalize()
#
# def test_data_transfer():
#     assert confirm_collection_size(destination, "27017", collections, datasize)
#
# def test_data_integrity():
#     assert compare_data_rs(source, destination)

