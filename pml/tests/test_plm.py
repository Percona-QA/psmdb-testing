import os
import re
import sys
import time

import pytest
import json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mlink.data_integrity_check import compare_data_rs
from mlink.cluster import Cluster

import testinfra.utils.ansible_runner
pml = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

def pml_start(host):
    try:
        output = json.loads(host.check_output("curl -s -X POST http://localhost:2242/start -d '{}'"))

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

def pml_finalize(host):
    try:
        output = json.loads(host.check_output("curl -s -X POST http://localhost:2242/finalize -d '{}'"))

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

def pml_status(host, timeout=45):
    try:
        output = host.check_output(f"curl -m {timeout} -s -X GET http://localhost:2242/status -d '{{}}'")
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

@pytest.fixture()
def pml_version(host):
    """Start and stop pbm-agent service

    :param host:
    :return:
    """
    result = host.run("percona-mongolink version")
    assert result.rc == 0, result.stdout
    return result

def pml_add_db_row(host):
    result = host.run("sudo docker exec -i source mongosh testdb --eval 'db.test.insertOne({ name: \"testUser\", age: 42 })'")
    assert result.rc == 0
    return True

def pml_confirm_db_row(host):
    result = host.run("sudo docker exec -i destination mongosh testdb --eval 'db.test.findOne()'")
    assert result.rc == 0
    return result

def wait_for_repl_stage(host, timeout=3600, interval=1, stable_duration=2):
    start_time = time.time()

    while time.time() - start_time < timeout:
        status_response = pml_status(host)

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
                stable_status = pml_status(host)
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

# def test_plm_binary(host):
#     """Check pbm binary
#     """
#     file = host.file("/tmp/percona-mongolink/bin/percona-mongolink")
#     assert file.user == "root"
#     assert file.group == "root"
#     try:
#         assert file.mode == 0o755
#     except AssertionError:
#         pytest.xfail("Possible xfail")
#
# def test_pml_version(pml_version):
#     """Check that pbm version is not empty strings
#
#     :param host:
#     :return:
#     """
#     pattern = r"^v\d+\.\d+ [a-f0-9]{7} \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
#
#     assert re.match(pattern, pml_version.stderr)
#
# def test_pml_help(host):
#     """Check that pbm have help message
#
#     :param host:
#     :return:
#     """
#     result = host.run("percona-mongolink help")
#     assert result.rc == 0, result.stdout

def test_pml_transfer(host):
    assert pml_add_db_row(host)
    assert pml_start(host)
    assert wait_for_repl_stage(host)
    assert "testUser" in pml_confirm_db_row(host).stdout
    assert pml_finalize(host)

def test_PML_data_integrity_PML_T42():
    success, mismatches = compare_data_rs(
        "mongodb://localhost:27017/test",  # source
        "mongodb://localhost:28017/test"   # destination
    )
    assert success, f"Data mismatch found: {mismatches}"

# def test_plm_status(pml_status):
#     pml_status_output = json.loads(pml_status.stdout)
#     assert "ok" in pml_status_output
#     assert "state" in pml_status_output
#     assert "info" in pml_status_output
#     assert "lagTime" in pml_status_output
#     assert "eventsProcessed" in pml_status_output
#     assert "lastReplicatedOpTime" in pml_status_output
#     assert "initialSync" in pml_status_output
#
#     assert pml_status_output["ok"] is True
#     assert pml_status_output["state"] == "running"
#     assert pml_status_output["info"] == "Replicating Changes"
#     assert pml_status_output["lagTime"] >= 0
#     assert isinstance(pml_status_output["eventsProcessed"], int)
#
#     sync = pml_status_output["initialSync"]
#     assert sync["completed"] is True
#     assert sync["cloneCompleted"] is True
#     assert sync["estimatedCloneSize"] >= 0
#     assert sync["clonedSize"] == sync["estimatedCloneSize"]

