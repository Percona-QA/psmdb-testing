import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import pytest
import requests
import testinfra.utils.ansible_runner

# PCSM-335 / PCSM-336: YYYY-MM-DDTHH:MM:SS.mmmZ (RFC 3339, UTC)
RFC3339_UTC_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
CONSOLE_RECORD = re.compile(
    r"^(?P<ts>.+?)\s+(?P<level>TRC|DBG|INF|WRN|ERR|FTL|PNC)\b"
)
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")

def _strip_ansi(text):
    return ANSI_ESCAPE.sub("", text or "")

pcsm = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

version = os.getenv("pcsm_version")
install_repo = os.getenv("install_repo")

def _pcsm_output(result):
    return f"{result.stdout or ''}{result.stderr or ''}"

def wait_until_pcsm_active(host, timeout=60, interval=1):
    """Wait until this instance holds the HA ACTIVE role

    After systemctl start/restart new process can be STANDBY until the
    previous instance's lease expires (LeaseTTL is 10s). /start and /status
    return error not_active on STANDBY process
    """
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        result = host.run("pcsm status")
        last = _pcsm_output(result).strip()
        stdout = (result.stdout or "").strip()
        if stdout:
            try:
                payload = json.loads(stdout)
                if payload.get("ok") is True:
                    return True
            except json.JSONDecodeError:
                pass
        time.sleep(interval)
    print(f"Timeout waiting for PCSM ACTIVE role: {last}")
    return False

def pcsm_start(host, timeout=60, interval=2):
    """Starts PCSM and waits until the endpoint is ready
    Also confirms the PCSM start command works and is ready to clone"""
    try:
        start = time.time()
        last_error = "unknown"
        while time.time() - start < timeout:
            result = host.run("pcsm start")
            raw_output = _pcsm_output(result)

            if "connection refused" in raw_output.lower():
                time.sleep(interval)
                continue

            try:
                output = json.loads(result.stdout)
            except (json.JSONDecodeError, TypeError):
                print("Received invalid JSON response.")
                time.sleep(interval)
                continue

            if output.get("ok") is True or output.get("error") == "already running":
                print("Sync started successfully")
                return True

            last_error = output.get("error", "Unknown error")
            # Restart left this process STANDBY until the old HA lease expires.
            if last_error == "not_active":
                print("PCSM is STANDBY (not_active); waiting to become ACTIVE")
                time.sleep(interval)
                continue

            print(f"Failed to start sync between src and dst cluster: {last_error}")
            return False

        print(f"Timeout: PCSM service did not become ready ({last_error}).")
        return False
    except (json.JSONDecodeError, OSError, AssertionError) as e:
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
    except (json.JSONDecodeError, OSError, AssertionError) as e:
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

    except (json.JSONDecodeError, OSError, AssertionError) as e:
        return {"success": False, "error": str(e)}

def pcsm_version(host):
    """Capture PCSM Version command and returns output"""
    result = host.run("pcsm version")
    assert result.rc == 0, result.stdout
    return result

def determine_release(host):
    distro = host.system_info.distribution.lower()
    release = host.system_info.release.split('.')[0]

    if distro == "rhel" and (release == "10" or release == "9"):
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
    assert wait_until_pcsm_active(host), "PCSM did not become ACTIVE after restart"
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
    assert wait_until_pcsm_active(host), "PCSM did not become ACTIVE after start"
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

def test_pcsm_version(host):
    """Test that pcsm version output is correct"""
    result = pcsm_version(host)
    lines = result.stderr.split("\n")
    parsed_config = {line.split(":")[0]: line.split(":")[1].strip() for line in lines[0:-1]}
    assert parsed_config['Version'] == f"v{version}", "Failed, actual version is " + parsed_config['Version']
    assert parsed_config['Platform'], "Failed, actual platform is " + parsed_config['Platform']
    assert parsed_config['GitCommit'] == get_git_commit(), f"GitCommit mismatch. Got '{parsed_config['GitCommit']}'"
    assert parsed_config['GitBranch'] == f"release-{version}", f"GitBranch mismatch. Got '{parsed_config['GitBranch']}'"
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

def _pcsm_journal_logs(host, timeout=15):
    """Return pcsm unit messages written in the last two minutes."""
    deadline = time.time() + timeout
    last_output = ""
    while time.time() < deadline:
        result = host.run(
            "sudo journalctl -u pcsm --no-pager -o cat --since=-2min"
        )
        last_output = _strip_ansi(result.stdout or result.stderr)
        if any(
            CONSOLE_RECORD.match(line.strip())
            for line in last_output.splitlines()
            if line.strip()
        ):
            return last_output
        time.sleep(0.5)
    raise AssertionError(
        f"No PCSM log records found in journal after start/restart:\n{last_output}"
    )

def test_pcsm_log_timestamps_rfc3339_utc(host):
    """PCSM-335/336: systemd journal captures RFC 3339 UTC timestamps from PCSM."""
    status = host.run("sudo systemctl is-active pcsm")
    if status.stdout.strip() == "active":
        restart_pcsm_service(host)
    else:
        start_pcsm_service(host)

    logs = _pcsm_journal_logs(host)
    timestamps = []
    for line in logs.splitlines():
        line = line.strip()
        if not line:
            continue
        match = CONSOLE_RECORD.match(line)
        if not match:
            # systemd unit messages (Started/Stopped/...) have no zerolog level
            continue
        timestamps.append((line, match.group("ts")))
    assert timestamps, f"No PCSM log records in journal:\n{logs}"

    now = datetime.now(timezone.utc)
    parsed_times = []
    for line, ts in timestamps:
        assert RFC3339_UTC_TS.match(ts), (
            f"PCSM-335/336: timestamp {ts!r} is not RFC 3339 UTC\nline: {line}"
        )
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        assert parsed.utcoffset() == timedelta(0), (
            f"PCSM-336: timestamp {ts!r} is not UTC\nline: {line}"
        )
        parsed_times.append((line, ts, parsed))

    _, last_ts, last_parsed = parsed_times[-1]
    assert abs((now - last_parsed).total_seconds()) < 300, (
        f"PCSM-336: latest timestamp {last_ts!r} is not near wall-clock UTC "
        f"(now={now.isoformat()})"
    )

def test_pcsm_transfer(host):
    """Test basic PCSM Transfer functionality"""
    assert pcsm_add_db_row(host)
    assert pcsm_start(host)
    assert wait_for_repl_stage(host)
    assert "testUser" in pcsm_confirm_db_row(host).stdout
    assert pcsm_finalize(host)

def test_pcsm_sbom(host):
    """Verify sbom exists, and the format and version are correct"""
    is_rpm = host.run("rpm -q percona-clustersync-mongodb").rc == 0
    is_deb = host.run("dpkg -l percona-clustersync-mongodb").rc == 0
    assert is_rpm or is_deb, "Could not detect package manager — package does not appear to be installed via rpm or deb"

    if is_rpm:
        result = host.run("rpm -ql percona-clustersync-mongodb | grep cdx.json")
    else:
        result = host.run("dpkg -L percona-clustersync-mongodb | grep cdx.json")
    assert result.rc == 0, f"SBOM cdx.json not found in package file list: {result.stdout}"

    sbom_path = f"/usr/share/doc/percona-clustersync-mongodb/percona-clustersync-mongodb-{version}.cdx.json"
    """
    if is_rpm:
        distro_map = {"rhel": "redhat", "amzn": "amazon"}
        distro_name = distro_map.get(host.system_info.distribution.lower(), host.system_info.distribution)
        distro = f"{distro_name}/{host.system_info.release}"
        trivy_result = host.run(f"trivy sbom --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 --distro {distro} {sbom_path}")
    else:
        trivy_result = host.run(f"trivy sbom --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 {sbom_path}")
    """
    trivy_result = host.run(f"trivy sbom --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 {sbom_path}")
    assert trivy_result.rc == 0, f"trivy sbom scan found HIGH/CRITICAL vulnerabilities:\n{trivy_result.stdout}\n{trivy_result.stderr}"

    cdx_cmd = "DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 /usr/local/bin/cyclonedx"
    cdx_result = host.run(f"{cdx_cmd} validate --input-file {sbom_path} --input-format json --input-version v1_6")
    assert cdx_result.rc == 0, f"CycloneDX 1.6 schema validation failed: {cdx_result.stdout}\n{cdx_result.stderr}"


