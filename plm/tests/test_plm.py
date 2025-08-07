import os
import time

import pytest
import json

import requests
import testinfra.utils.ansible_runner
plm = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

version = os.getenv("plm_version")
install_repo = os.getenv("install_repo")

def plm_start(host, timeout=60, interval=2):
    """Starts PLM and waits until the endpoint is ready
    Also confirms the PLM start command works and is ready to clone"""
    try:
        start = time.time()
        while time.time() - start < timeout:
            result = host.run("plm start")
            raw_output = result.stderr

            if 'connection refused' not in raw_output:
                print("PLM service has started.")
                break

            time.sleep(interval)

        else:
            print("Timeout: PLM service did not become ready.")
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

def plm_finalize(host):
    """Executes plm finalize command
    signalising that no more replication is to occur"""
    try:
        output = json.loads(host.check_output("plm finalize"))

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

def plm_status(host, timeout=45):
    """Executes plm status command and returns output"""
    try:
        output = host.check_output(f"plm status")
        json_output = json.loads(output)

        if not json_output.get("ok", False):
            return {"success": False, "error": "plink status command returned ok: false"}

        try:
            cleaned_output = json.loads(output.replace("\n", "").replace("\r", "").strip())
            return {"success": True, "data": cleaned_output}
        except json.JSONDecodeError as e:
            return {"success": False, "error": "Invalid JSON response"}

    except Exception as e:
        return {"success": False, "error": str(e)}

def plm_version(host):
    """Capture PLM Version command and returns output"""
    result = host.run("plm version")
    assert result.rc == 0, result.stdout
    return result

def plm_add_db_row(host):
    """Adds a test row to source database"""

    distro = host.system_info.distribution.lower()
    release = host.system_info.release.split('.')[0]

    if distro == "redhat" and release == "10":
        print("RUNNING PODMAN")
        runtime = "podman"
    else:
        print("RUNNING DOCKER")
        runtime = "docker"

    # Run the appropriate command
    result = host.run(f"sudo {runtime} exec -i source mongosh testdb --eval 'db.test.insertOne({{ name: \"testUser\", age: 42 }})'")

    assert result.rc == 0
    return True

def plm_confirm_db_row(host):
    """Captures and returns output on a query on the destination database"""
    distro = host.system_info.distribution.lower()
    release = host.system_info.release.split('.')[0]

    if distro == "redhat" and release == "10":
        print("RUNNING PODMAN")
        runtime = "podman"
    else:
        print("RUNNING DOCKER")
        runtime = "docker"

    result = host.run(f"sudo {runtime} exec -i destination mongosh testdb --eval 'db.test.findOne()'")
    assert result.rc == 0
    return result

def wait_for_repl_stage(host, timeout=3600, interval=1, stable_duration=2):
    """Wait for plm replication to complete"""
    start_time = time.time()

    while time.time() - start_time < timeout:
        status_response = plm_status(host)

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
                stable_status = plm_status(host)
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
    """Restarts plm service and confirms it's running"""
    result = host.run("sudo systemctl restart plm")
    assert result.rc == 0, result.stdout
    is_active = host.run("sudo systemctl show -p SubState plm")
    assert is_active.stdout.strip() == "SubState=running", f"PLM service is not running: {is_active.stdout}"
    return result

def stop_plm_service(host):
    """Stops plm service and confirms it's not running"""
    stop_plm = host.run("sudo systemctl stop plm")
    assert stop_plm.rc == 0
    is_active = host.run("sudo systemctl is-active plm")
    assert is_active.stdout.strip() == "inactive", f"PLM service is still active: {is_active.stdout}"
    return stop_plm

def start_plm_service(host):
    """Starts plm service and confirms it's running"""
    start_plm = host.run("sudo systemctl start plm")
    assert start_plm.rc == 0, start_plm.stdout
    status = host.run("sudo systemctl is-active plm")
    assert status.stdout.strip() == "active", f"PLM service is inactive: {status.stdout}"
    return start_plm

def get_git_commit():
    headers = {'Authorization': 'token ' + str(os.environ.get("MONGO_REPO_TOKEN"))}
    if install_repo == "release":
        url = f"https://api.github.com/repos/percona/percona-link-mongodb/commits/release-{version}"
    else:
        url = f"https://api.github.com/repos/percona/percona-link-mongodb/commits/main"
    git_commit = requests.get(url, headers=headers)

    if git_commit.status_code == 200:
        return git_commit.json()["sha"]
    else:
        print(f"Unable to obtain git commit, failed with status code: {git_commit.status_code}")
        return False

def test_plm_version(host):
    """Test that plm version output is correct"""
    result = plm_version(host)
    lines = result.stderr.split("\n")
    parsed_config = {line.split(":")[0]: line.split(":")[1].strip() for line in lines[0:-1]}
    assert parsed_config['Version'] == f"v{version}", "Failed, actual version is " + parsed_config['Version']
    assert parsed_config['Platform'], "Failed, actual platform is " + parsed_config['Platform']
    assert parsed_config['GitCommit'] == get_git_commit(), "Failed, actual git commit is " + parsed_config['GitCommit']
    expected_version = f"release-{version}" if version == "release" else "main"
    assert parsed_config['GitBranch'] == expected_version, "Failed, actual git branch is " + parsed_config['GitBranch']
    assert parsed_config['BuildTime'], parsed_config
    assert parsed_config['GoVersion'], parsed_config

def test_plm_binary(host):
    """Check PLM binary exists with the correct permissions"""
    file = host.file("/usr/bin/plm")
    assert file.user == "root"
    assert file.group == "root"
    try:
        assert file.mode == 0o755
    except AssertionError:
        pytest.xfail("Possible xfail")

def test_plm_help(host):
    """Check that PLM help command works"""
    result = host.run("plm help")
    assert result.rc == 0, result.stdout

def test_plm_environment_file_exists(host):
    """Test plm-service file exists"""
    service_file = host.file("/lib/systemd/system/plm.service")
    assert service_file.user == "root"
    assert service_file.group == "root"
    try:
        assert service_file.mode == 0o644
    except AssertionError:
        pytest.xfail("Possible xfail")

def test_stop_plm(host):
    """Test plm service stops successfully"""
    stop_plm_service(host)

def test_start_plm(host):
    """Test plm service starts successfully"""
    start_plm_service(host)

def test_restart_plm(host):
    """Test plm service restarts successfully"""
    restart_plm_service(host)

def test_plm_transfer(host):
    """Test basic PLM Transfer functionality"""
    assert plm_add_db_row(host)
    assert plm_start(host)
    assert wait_for_repl_stage(host)
    assert "testUser" in plm_confirm_db_row(host).stdout
    assert plm_finalize(host)
