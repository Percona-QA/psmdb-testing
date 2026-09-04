import concurrent.futures
import json
import os
import time
from datetime import datetime

import testinfra.utils.ansible_runner

from data_integrity_check import compare_data_rs

source = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('sharded-pcsm-source')

destination = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('sharded-pcsm-destination')

destination_b = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('sharded-pcsm-destination-b')

pcsm = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_host('sharded-pcsm-clustersync')

collections = int(os.getenv("COLLECTIONS", default = 5))
datasize = int(os.getenv("DATASIZE", default = 100))
distribute = os.getenv("RANDOM_DISTRIBUTE_DATA", default="false").lower() == "true"
doc_template = os.getenv("DOC_TEMPLATE", default = 'random')
FULL_DATA_COMPARE = os.getenv("FULL_DATA_COMPARE", default="false").lower() == "true"
TIMEOUT = int(os.getenv("TIMEOUT", default=3600))

# PCSM-330: two independent syncs run off the same sharded source, each carrying a
# different database to its own sharded target. TARGETS maps each PCSM instance
# (its own HTTP API port and systemd unit) to the db it owns and the destination
# mongos host it lands on.
TARGETS = {
    "a": {"port": 2242, "dbname": "db_0", "destination": destination},
    "b": {"port": 2243, "dbname": "db_1", "destination": destination_b},
}

def load_data(node, dbname):
    env_vars = (
        f"COLLECTIONS={collections} DATASIZE={datasize} DISTRIBUTE={distribute} "
        f"DOC_TEMPLATE={doc_template} DBNAME={dbname}"
    )
    node.run_test(f"{env_vars} python3 /tmp/load_data.py --port 27018")

def obtain_pcsm_address(node):
    ipaddress = node.check_output(
        "ip -4 addr show scope global | grep inet | awk '{print $2}' | cut -d/ -f1 | head -n 1")
    return ipaddress

def confirm_collection_size(node, datasize, dbname="test_db", port="27018"):
    cmd = (
        f'mongosh "mongodb://127.0.0.1:{port}/" --quiet --eval \''
        f'let total = 0; '
        f'const dbname = "{dbname}"; const targetdb = db.getSiblingDB(dbname); '
        f'targetdb.getCollectionNames().forEach(name => {{ '
        f'  let stats = targetdb.getCollection(name).stats(); '
        f'  if (stats && typeof stats.size === "number") {{ '
        f'    const orphanCount = stats.numOrphanDocs || 0; '
        f'    const avg = stats.avgObjSize || 0; '
        f'    const effectiveSize = stats.size - (orphanCount * avg); '
        f'    total += effectiveSize; '
        f'  }} '
        f'}}); '
        f'print((total / (1024 * 1024)).toFixed(2));\''
    )

    try:
        result = node.check_output(cmd)
        size_mb = float(result.strip())
        lower_bound = datasize * 0.995
        upper_bound = datasize * 1.005
        return lower_bound <= size_mb <= upper_bound
    except Exception:
        return False

def confirm_db_absent(node, dbname, port="27018"):
    cmd = (
        f'mongosh "mongodb://127.0.0.1:{port}/" --quiet --eval '
        f'\'print(db.getSiblingDB("{dbname}").getCollectionNames().length);\'')
    try:
        result = node.check_output(cmd)
        return int(result.strip()) == 0
    except Exception:
        return False

def pcsm_start(port=2242, include_namespaces=None):
    try:
        # includeNamespaces is a start-request body field, not a daemon CLI flag -
        # the pcsm binary run by the systemd unit only accepts --source/--target/
        # --port etc.; per-target namespace scoping has to happen here, at start time.
        body = {"includeNamespaces": include_namespaces} if include_namespaces else {}
        output = json.loads(pcsm.check_output(
            f"curl -s -X POST http://localhost:{port}/start -d '{json.dumps(body)}'"))

        if output:
            try:
                if output.get("ok") is True or output.get("error") == "already running":
                    print(f"Sync started successfully on port {port}")
                    return True

                elif output.get("ok") is False and output.get("error") != "already running":
                    error_msg = output.get("error", "Unknown error")
                    print(f"Failed to start sync between src and dst cluster on port {port}: {error_msg}")
                    return False

            except json.JSONDecodeError:
                print("Received invalid JSON response.")

        print(f"Failed to start sync between src and dst cluster on port {port}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def pcsm_finalize(port=2242):
    try:
        output = json.loads(pcsm.check_output(f"curl -s -X POST http://localhost:{port}/finalize -d '{{}}'"))

        if output:
            try:
                print(output)
                if output.get("ok") is True:
                    print(f"Sync finalized successfully on port {port}")
                    return True

                elif output.get("ok") is False:
                    error_msg = output.get("error", "Unknown error")
                    print(f"Failed to finalize sync between src and dst cluster on port {port}: {error_msg}")
                    return False

            except json.JSONDecodeError:
                print("Received invalid JSON response.")

        print(f"Failed to finalize sync between src and dst cluster on port {port}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def status(port=2242, timeout=45):
    try:
        output = pcsm.check_output(f"curl -m {timeout} -s -X GET http://localhost:{port}/status -d '{{}}'")
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

def wait_for_repl_stage(port=2242, timeout=3600, interval=1, stable_duration=2):
    start_time = time.time()

    while time.time() - start_time < timeout:
        status_response = status(port=port)

        if not status_response["success"]:
            print(f"Error: Impossible to retrieve status on port {port}, {status_response['error']}")
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
                stable_status = status(port=port)
                if not stable_status["success"]:
                    print(f"Error: Impossible to retrieve status on port {port}, {stable_status['error']}")
                    return False

                state = stable_status["data"].get("state")
                if state != "running":
                    return False
                time.sleep(0.5)
            elapsed = round(time.time() - start_time, 2)
            print(f"Initial sync completed on port {port} in {elapsed} seconds")
            return True
        time.sleep(interval)

    print(f"Error: Timeout reached while waiting for initial sync to complete on port {port}")
    return False

def log_step(message):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def test_prepare_data():
    # PCSM-330: db_0 and db_1 are independent databases on the same source cluster,
    # so there's no reason to generate them one after another - that just doubles
    # the wall-clock time for no benefit. Kick both off at once instead.
    dbnames = ", ".join(target["dbname"] for target in TARGETS.values())
    log_step(f"Starting data generation on source node for both targets ({dbnames})...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(TARGETS)) as executor:
        futures = {
            executor.submit(load_data, source, target["dbname"]): name
            for name, target in TARGETS.items()
        }
        for future in concurrent.futures.as_completed(futures):
            future.result()  # re-raise if load_data failed for this target
    log_step("Data generation completed for both targets. Validating sizes...")

    for name, target in TARGETS.items():
        assert confirm_collection_size(source, datasize, dbname=target["dbname"]), \
            f"Source data size validation failed for {target['dbname']}"
        log_step(f"Source data size confirmed for {target['dbname']}")

def test_data_transfer_PCSM_330():
    log_step("Starting PCSM syncs for both targets...")
    for name, target in TARGETS.items():
        assert pcsm_start(port=target["port"], include_namespaces=[f"{target['dbname']}.*"]), \
            f"Failed to start sync for target {name}"

    log_step("Waiting for replication to complete on both targets...")
    for name, target in TARGETS.items():
        assert wait_for_repl_stage(port=target["port"], timeout=TIMEOUT), \
            f"Replication did not complete for target {name}"

    log_step("Finalizing sync on both targets...")
    for name, target in TARGETS.items():
        assert pcsm_finalize(port=target["port"]), f"PCSM sync did not complete successfully for target {name}"
    log_step("Both PCSM syncs completed successfully")

def test_datasize_PCSM_330():
    for name, target in TARGETS.items():
        log_step(f"Validating destination data size for target {name} (db {target['dbname']})...")
        assert confirm_collection_size(target["destination"], datasize, dbname=target["dbname"]), \
            f"Destination data size validation failed for target {name}"
        log_step(f"Destination data size confirmed for target {name}")

def _assert_only_own_subset_synced(target_name, target, other_dbname):
    """
    Mirrors pcsm-pytest's _assert_only_own_subset_synced (test_multi_target_sync.py):
    run the full, unscoped source-vs-destination comparison rather than scoping it
    to the target's own db, then interpret the result - the other sync's db is
    *expected* to show up as "missing in dst DB" (that's the disjoint-sync signal
    working correctly), but nothing else should be mismatched. A direct absence
    check backs that up in case compare_data_rs's own db enumeration ever misses it.
    """
    result, mismatches = compare_data_rs(source, target["destination"], "27018", FULL_DATA_COMPARE)
    if result:
        raise AssertionError(
            f"Target {target_name}: expected {other_dbname} to be reported missing, "
            f"but compare_data_rs found no mismatches at all")

    unexpected = [(name, reason) for name, reason in mismatches if not name.startswith(other_dbname)]
    assert not unexpected, \
        f"Target {target_name}: unexpected mismatch in its own subset ({target['dbname']}): {unexpected}"
    assert any(name.startswith(other_dbname) for name, _ in mismatches), \
        f"Target {target_name}: {other_dbname} was not reported as missing by compare_data_rs: {mismatches}"

    assert confirm_db_absent(target["destination"], other_dbname), \
        f"Target {target_name}: unexpectedly contains {other_dbname}, which belongs to the other sync"

def test_data_integrity_and_disjoint_sync_PCSM_330():
    other = {"a": "b", "b": "a"}
    for name, target in TARGETS.items():
        other_dbname = TARGETS[other[name]]["dbname"]
        log_step(f"Comparing data integrity between source and target {name} "
                 f"(own db {target['dbname']}, expecting {other_dbname} to be absent)...")
        _assert_only_own_subset_synced(name, target, other_dbname)
        log_step(f"Confirmed target {name} has exactly its own subset ({target['dbname']}), "
                 f"with {other_dbname} correctly absent")
