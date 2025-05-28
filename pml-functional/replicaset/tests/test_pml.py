import os
import random
import time
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
TIMEOUT = int(os.getenv("TIMEOUT",default = 3600))

def get_cpu_count():
    total_cpus = pml.check_output("nproc")
    print("The CPUs that are going to be used are: " + str(total_cpus/2))
    return int(total_cpus)/2

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
    node.check_output(f'mgodatagen -n {get_cpu_count()} --uri=mongodb://127.0.0.1:' + port + '/?replicaSet=rs -f /tmp/generated_config.json --batchsize 10')

def check_count_data(node,port):
    result = node.check_output("mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval 'db.binary.count()' --quiet | tail -1")
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
                    print("Sync started successfully")
                    return True

                elif output.get("ok") is False and output.get("error") != "already running":
                    error_msg = output.get("error", "Unknown error")
                    print(f"Failed to start sync between src and dst cluster: {error_msg}")
                    return False

            except json.JSONDecodeError:
                print("Received invalid JSON response.")

        print("Failed to start sync between src and dst cluster")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def pml_finalize():
    try:
        output = json.loads(pml.check_output("curl -s -X POST http://localhost:2242/finalize -d '{}'"))

        if output:
            try:
                print(output)
                if output.get("ok") is True:
                    print("Sync finalized successfully")
                    return True

                elif output.get("ok") is False:
                    error_msg = output.get("error", "Unknown error")
                    print(f"Failed to finalize sync between src and dst cluster: {error_msg}")
                    return False

            except json.JSONDecodeError:
                print("Received invalid JSON response.")

        print("Failed to finalize sync between src and dst cluster")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def status(timeout=45):
    try:
        output = pml.check_output(f"curl -m {timeout} -s -X GET http://localhost:2242/status -d '{{}}'")
        json_output = json.loads(output)
        print(output)

        if not json_output.get("ok", False):
            return {"success": False, "error": "mlink status command returned ok: false"}

        try:
            cleaned_output = json.loads(output.replace("\n", "").replace("\r", "").strip())
            return {"success": True, "data": cleaned_output}
        except json.JSONDecodeError as e:
            return {"success": False, "error": "Invalid JSON response"}

    except Exception as e:
        return {"success": False, "error": str(e)}

def wait_for_repl_stage(timeout=3600, interval=1, stable_duration=2):
    start_time = time.time()

    while time.time() - start_time < timeout:
        status_response = status()

        if not status_response["success"]:
            print(f"Error: Impossible to retrieve status, {status_response['error']}")
            return False

        initial_sync = status_response["data"].get("initialSync")
        if initial_sync is None:
            time.sleep(interval)
            continue
        if "completed" not in initial_sync:
            time.sleep(interval)
            continue
        if initial_sync["completed"]:
            stable_start = time.time()
            while time.time() - stable_start < stable_duration:
                stable_status = status()
                if not stable_status["success"]:
                    print(f"Error: Impossible to retrieve status, {stable_status['error']}")
                    return False

                state = stable_status["data"].get("state")
                if state != "running":
                    return False
                time.sleep(0.5)
            elapsed = round(time.time() - start_time, 2)
            print(f"Initial sync completed in {elapsed} seconds")
            return True
        time.sleep(interval)

    print("Error: Timeout reached while waiting for initial sync to complete")
    return False

def test_prepare_data():
    load_data(source,"27017")
    assert confirm_collection_size(source, "27017", collections, datasize)

def test_data_transfer_PML_T40():
    assert pml_start()
    assert wait_for_repl_stage(TIMEOUT)
    assert pml_finalize()
    assert confirm_collection_size(destination, "27017", collections, datasize)

def test_datasize_PML_T41():
    assert confirm_collection_size(destination, "27017", collections, datasize)

def test_PML_data_integrity_PML_T42():
    assert compare_data_rs(source, destination, "27017")

