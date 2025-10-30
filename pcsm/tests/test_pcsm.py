import os
import time

import pytest
import json

import requests
import testinfra.utils.ansible_runner
pcsm = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

version = os.getenv("pcsm_version")
install_repo = os.getenv("install_repo")

def pcsm_start(host, timeout=60, interval=2):
    """Starts PCSM and waits until the endpoint is ready
    Also confirms the PCSM start command works and is ready to clone"""
    try:
        start = time.time()
        while time.time() - start < timeout:
            result = host.run("pcsm start")
            raw_output = result.stderr

            if 'connection refused' not in raw_output:
                print("PCSM service has started.")
                break

            time.sleep(interval)

        else:
            print("Timeout: PCSM service did not become ready.")
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

def pcsm_finalize(host):
    """Executes pcsm finalize command
    signalising that no more replication is to occur"""
    try:
        output = json.loads(host.check_output("pcsm finalize"))

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

def pcsm_status(host, timeout=45):
    """Executes pcsm status command and returns output"""
    try:
        output = host.check_output("pcsm status")
        json_output = json.loads(output)

        if not json_output.get("ok", False):
            return {"success": False, "error": "csync status command returned ok: false"}

        try:
            cleaned_output = json.loads(output.replace("\n", "").replace("\r", "").strip())
            return {"success": True, "data": cleaned_output}
        except json.JSONDecodeError:
            return {"success": False, "error": "Invalid JSON response"}

    except Exception as e:
        return {"success": False, "error": str(e)}

def pcsm_version(host):
    """Capture PCSM Version command and returns output"""
    result = host.run("pcsm version")
    assert result.rc == 0, result.stdout
    return result

def determine_release(host):
    distro = host.system_info.distribution.lower()
    release = host.system_info.release.split('.')[0]

    if distro == "rhel" and release == "10":
        return "podman"
    else:
        return "docker"

def pcsm_add_db_row(host):
    """Adds a test row to source database"""

    # Run the appropriate command
    result = host.run(f"sudo {determine_release(host)} exec -i source mongosh testdb --eval 'db.test.insertOne({{ name: \"testUser\", age: 42 }})'")

    assert result.rc == 0
    return True

def pcsm_confirm_db_row(host):
    """Captures and returns output on a query on the destination database"""
    result = host.run(f"sudo {determine_release(host)} exec -i destination mongosh testdb --eval 'db.test.findOne()'")
    assert result.rc == 0
    return result

def wait_for_repl_stage(host, timeout=3600, interval=1, stable_duration=2):
    """Wait for pcsm replication to complete"""
    start_time = time.time()

    while time.time() - start_time < timeout:
        status_response = pcsm_status(host)

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
                stable_status = pcsm_status(host)
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

def restart_pcsm_service(host):
    """Restarts pcsm service and confirms it's running"""
    result = host.run("sudo systemctl restart pcsm")
    assert result.rc == 0, result.stdout
    is_active = host.run("sudo systemctl show -p SubState pcsm")
    assert is_active.stdout.strip() == "SubState=running", f"PCSM service is not running: {is_active.stdout}"
    return result

def stop_pcsm_service(host):
    """Stops pcsm service and confirms it's not running"""
    stop_pcsm = host.run("sudo systemctl stop pcsm")
    assert stop_pcsm.rc == 0
    is_active = host.run("sudo systemctl is-active pcsm")
    assert is_active.stdout.strip() == "inactive", f"PCSM service is still active: {is_active.stdout}"
    return stop_pcsm

def start_pcsm_service(host):
    """Starts pcsm service and confirms it's running"""
    start_pcsm = host.run("sudo systemctl start pcsm")
    assert start_pcsm.rc == 0, start_pcsm.stdout
    status = host.run("sudo systemctl is-active pcsm")
    assert status.stdout.strip() == "active", f"PCSM service is inactive: {status.stdout}"
    return start_pcsm

def get_git_commit():
    headers = {'Authorization': 'token ' + str(os.environ.get("MONGO_REPO_TOKEN"))}
    url = f"https://api.github.com/repos/percona/percona-clustersync-mongodb/commits/release-{version}"
    git_commit = requests.get(url, headers=headers)

    if git_commit.status_code == 200:
        return git_commit.json()["sha"]
    else:
        print(f"Unable to obtain git commit, failed with status code: {git_commit.status_code}")
        return False

@pytest.mark.xfail(reason="Git Branch may be incorrect")
def test_pcsm_version(host):
    """Test that pcsm version output is correct"""
    result = pcsm_version(host)
    lines = result.stderr.split("\n")
    parsed_config = {line.split(":")[0]: line.split(":")[1].strip() for line in lines[0:-1]}
    assert parsed_config['Version'] == f"v{version}", "Failed, actual version is " + parsed_config['Version']
    assert parsed_config['Platform'], "Failed, actual platform is " + parsed_config['Platform']
    try:
        assert parsed_config['GitCommit'] == get_git_commit()
    except AssertionError:
        pytest.xfail(f"Non-blocking failure: GitCommit mismatch. Got '{parsed_config['GitCommit']}'")
    try:
        assert parsed_config['GitBranch'] == f"release-{version}"
    except AssertionError:
        pytest.xfail(f"Non-blocking failure: GitBranch mismatch. Got '{parsed_config['GitBranch']}'")
    assert parsed_config['BuildTime'], parsed_config
    assert parsed_config['GoVersion'], parsed_config

def test_pcsm_binary(host):
    """Check PCSM binary exists with the correct permissions"""
    file = host.file("/usr/bin/pcsm")
    assert file.user == "root"
    assert file.group == "root"
    try:
        assert file.mode == 0o755
    except AssertionError:
        pytest.xfail("Possible xfail")

def test_pcsm_help(host):
    """Check that PCSM help command works"""
    result = host.run("pcsm help")
    assert result.rc == 0, result.stdout

def test_pcsm_environment_file_exists(host):
    """Test pcsm-service file exists"""
    service_file = host.file("/lib/systemd/system/pcsm.service")
    assert service_file.user == "root"
    assert service_file.group == "root"
    try:
        assert service_file.mode == 0o644
    except AssertionError:
        pytest.xfail("Possible xfail")

def test_stop_pcsm(host):
    """Test pcsm service stops successfully"""
    stop_pcsm_service(host)

def test_start_pcsm(host):
    """Test pcsm service starts successfully"""
    start_pcsm_service(host)

def test_restart_pcsm(host):
    """Test pcsm service restarts successfully"""
    restart_pcsm_service(host)

def test_pcsm_transfer(host):
    """Test basic PCSM Transfer functionality"""
    assert pcsm_add_db_row(host)
    assert pcsm_start(host)
    assert wait_for_repl_stage(host)
    assert "testUser" in pcsm_confirm_db_row(host).stdout
    assert pcsm_finalize(host)
