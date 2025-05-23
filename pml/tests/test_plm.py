import os
import re
import time

import pytest
import json

import testinfra.utils.ansible_runner
pml = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

def pml_start(host, timeout=60, interval=2):
    """Starts PML and waits until the endpoint is ready
    Also confirms the PML start command works and is ready to clone"""
    try:
        start = time.time()
        while time.time() - start < timeout:
            result = host.run("percona-mongolink start")
            raw_output = result.stderr

            if 'connection refused' not in raw_output:
                print("PML service has started.")
                break

            time.sleep(interval)

        else:
            print("Timeout: PML service did not become ready.")
            return False

        output = json.loads(result.stdout)

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
    """Executes percona-mongolink finalize command
    signalising that no more replication is to occur"""
    try:
        output = json.loads(host.check_output("percona-mongolink finalize"))

        if output:
            try:
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
    """Executes percona-mongolink status command and returns output"""
    try:
        output = host.check_output(f"percona-mongolink status")
        json_output = json.loads(output)

        if not json_output.get("ok", False):
            return {"success": False, "error": "mlink status command returned ok: false"}

        try:
            cleaned_output = json.loads(output.replace("\n", "").replace("\r", "").strip())
            return {"success": True, "data": cleaned_output}
        except json.JSONDecodeError as e:
            return {"success": False, "error": "Invalid JSON response"}

    except Exception as e:
        return {"success": False, "error": str(e)}

def pml_version(host):
    """Capture PLM Version command and returns output"""
    result = host.run("percona-mongolink version")
    assert result.rc == 0, result.stdout
    return result

def pml_add_db_row(host):
    """Adds a test row to source database"""
    result = host.run("sudo docker exec -i source mongosh testdb --eval 'db.test.insertOne({ name: \"testUser\", age: 42 })'")
    assert result.rc == 0
    return True

def pml_confirm_db_row(host):
    """Captures and returns output on a query on the destination database"""
    result = host.run("sudo docker exec -i destination mongosh testdb --eval 'db.test.findOne()'")
    assert result.rc == 0
    return result

def wait_for_repl_stage(host, timeout=3600, interval=1, stable_duration=2):
    """Wait for percona-mongolink replication to complete"""
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

def restart_plm_service(host):
    """Restarts percona-mongolink service and confirms it's running"""
    result = host.run("sudo systemctl restart percona-mongolink")
    assert result.rc == 0, result.stdout
    is_active = host.run("sudo systemctl show -p SubState percona-mongolink")
    assert is_active.stdout.strip() == "SubState=running", f"PLM service is not running: {is_active.stdout}"
    return result

def stop_plm_service(host):
    """Stops percona-mongolink service and confirms it's not running"""
    stop_plm = host.run("sudo systemctl stop percona-mongolink")
    assert stop_plm.rc == 0
    is_active = host.run("sudo systemctl is-active percona-mongolink")
    assert is_active.stdout.strip() == "inactive", f"PLM service is still active: {is_active.stdout}"
    return stop_plm

def start_plm_service(host):
    """Starts percona-mongolink service and confirms it's running"""
    start_plm = host.run("sudo systemctl start percona-mongolink")
    assert start_plm.rc == 0, start_plm.stdout
    status = host.run("sudo systemctl is-active percona-mongolink")
    assert status.stdout.strip() == "active", f"PLM service is inactive: {status.stdout}"
    return start_plm

def test_pml_version(host):
    """Test that percona-mongolink version output is correct"""
    result = pml_version(host)
    lines = result.stdout.split("\n")
    parsed_config = {line.split(":")[0]: line.split(":")[1].strip() for line in lines[0:-1]}
    print("KEITH TEST: " + stre(parsed_config))
    assert parsed_config['Version'], parsed_config
    assert parsed_config['Platform'], parsed_config
    assert parsed_config['GitCommit'], parsed_config
    assert parsed_config['GitBranch'], parsed_config
    assert parsed_config['BuildTime'], parsed_config
    assert parsed_config['GoVersion'], parsed_config

# def test_plm_binary(host):
#     """Check PLM binary exists with the correct permissions"""
#     file = host.file("/usr/bin/percona-mongolink")
#     assert file.user == "root"
#     assert file.group == "root"
#     try:
#         assert file.mode == 0o755
#     except AssertionError:
#         pytest.xfail("Possible xfail")
#
# def test_pml_help(host):
#     """Check that PLM help command works"""
#     result = host.run("percona-mongolink help")
#     assert result.rc == 0, result.stdout
#
# def test_pml_environment_file_exists(host):
#     """Test percona-mongolink-service file exists"""
#     service_file = host.file("/lib/systemd/system/percona-mongolink.service")
#     assert service_file.user == "root"
#     assert service_file.group == "root"
#     try:
#         assert service_file.mode == 0o644
#     except AssertionError:
#         pytest.xfail("Possible xfail")
#
# def test_stop_pml(host):
#     """Test percona-mongolink service stops successfully"""
#     stop_plm_service(host)
#
# def test_start_pml(host):
#     """Test percona-mongolink service starts successfully"""
#     start_plm_service(host)
#
# def test_restart_pml(host):
#     """Test percona-mongolink service restarts successfully"""
#     restart_plm_service(host)
#
# def test_pml_transfer(host):
#     """Test basic PLM Transfer functionality"""
#     assert pml_add_db_row(host)
#     assert pml_start(host)
#     assert wait_for_repl_stage(host)
#     assert "testUser" in pml_confirm_db_row(host).stdout
#     assert pml_finalize(host)
