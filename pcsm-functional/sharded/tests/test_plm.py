import os
import time
import json
from datetime import datetime
import testinfra.utils.ansible_runner

from data_integrity_check import compare_data_rs

source = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('sharded-pcsm-source')

destination = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('sharded-pcsm-destination')

pcsm = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('sharded-pcsm-clustersync')

collections = int(os.getenv("COLLECTIONS", default = 5))
datasize = int(os.getenv("DATASIZE", default = 100))
distribute = os.getenv("RANDOM_DISTRIBUTE_DATA", default="false").lower() == "true"
doc_template = os.getenv("DOC_TEMPLATE", default = 'random')
FULL_DATA_COMPARE = os.getenv("FULL_DATA_COMPARE", default="false").lower() == "true"
TIMEOUT = int(os.getenv("TIMEOUT",default = 3600))

def load_data(node):
    env_vars = f"COLLECTIONS={collections} DATASIZE={datasize} DISTRIBUTE={distribute} DOC_TEMPLATE={doc_template}"
    node.run_test(f"{env_vars} python3 /tmp/load_data.py --port 27018")

def obtain_pcsm_address(node):
    ipaddress = node.check_output(
        "ip -4 addr show scope global | grep inet | awk '{print $2}' | cut -d/ -f1 | head -n 1")
    return ipaddress

def confirm_collection_size(node, datasize, dbname="test_db", port="27018"):
    cmd = (
        f'mongosh "mongodb://127.0.0.1:{port}/" --quiet --eval \'let total = 0; '
        f'const dbname = "{dbname}"; const targetdb = db.getSiblingDB(dbname); '
        f'targetdb.getCollectionNames().forEach(name => {{ '
        f'let stats = targetdb.getCollection(name).stats(); '
        f'if (stats && typeof stats.size === "number") {{ total += stats.size; }} }}); '
        f'print((total / (1024 * 1024)).toFixed(2));\'')

    try:
        result = node.check_output(cmd)
        size_mb = float(result.strip())
        lower_bound = datasize * 0.995
        upper_bound = datasize * 1.005
        return lower_bound <= size_mb <= upper_bound
    except Exception:
        return False

def pcsm_start():
    try:
        output = json.loads(pcsm.check_output("curl -s -X POST http://localhost:2242/start -d '{}'"))

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

def pcsm_finalize():
    try:
        output = json.loads(pcsm.check_output("curl -s -X POST http://localhost:2242/finalize -d '{}'"))

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
        output = pcsm.check_output(f"curl -m {timeout} -s -X GET http://localhost:2242/status -d '{{}}'")
        json_output = json.loads(output)
        print(output)

        if not json_output.get("ok", False):
            return {"success": False, "error": "csync status command returned ok: false"}

        try:
            cleaned_output = json.loads(output.replace("\n", "").replace("\r", "").strip())
            return {"success": True, "data": cleaned_output}
        except json.JSONDecodeError:
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

def log_step(message):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def test_prepare_data():
    log_step("Starting data generation on source node...")
    load_data(source)
    log_step("Data generation completed. Validating size...")
    assert confirm_collection_size(source, datasize), "Source data size validation failed"
    log_step("Source data size confirmed")

# def test_data_transfer_PML_T60():
#     log_step("Starting PCSM sync...")
#     assert pcsm_start()
#     log_step("Waiting for replication to complete...")
#     assert wait_for_repl_stage(TIMEOUT)
#     log_step("Finalizing sync...")
#     assert pcsm_finalize(), "PCSM sync did not complete successfully"
#     log_step("PCSM sync completed successfully")
#
# def test_datasize_PML_T61():
#     log_step("Validating destination data size...")
#     assert confirm_collection_size(destination, datasize), "Destination data size validation failed"
#     log_step("Destination data size confirmed")
#
# def test_data_integrity_PML_T62():
#     log_step("Comparing data integrity between source and destination...")
#     result, _ = compare_data_rs(source, destination, "27018", FULL_DATA_COMPARE)
#     assert result is True, "Data mismatch after synchronization"
#     log_step("Data integrity check completed successfully")
