import time

import pytest
import requests
from cluster import Cluster
from clustersync import Clustersync
from conftest import get_cluster_config

import docker


@pytest.fixture(scope="module")
def src_cluster():
    """Create src cluster once per module"""
    config = get_cluster_config("replicaset")
    cluster = Cluster(config['src_config'], mongo_image="mongodb-src/local")
    cluster.create()
    yield cluster
    cluster.destroy()

@pytest.fixture(scope="module")
def dst_cluster():
    """Create dst cluster once per module"""
    config = get_cluster_config("replicaset")
    cluster = Cluster(config['dst_config'], mongo_image="mongodb-dst/local")
    cluster.create()
    yield cluster
    cluster.destroy()

@pytest.fixture(scope="function")
def csync(src_cluster, dst_cluster, csync_env):
    """Recreate only PCSM container with new env vars for each test"""
    csync = Clustersync('csync',
                        src_cluster.csync_connection,
                        dst_cluster.csync_connection)
    csync.create(extra_args="--reset-state", env_vars=csync_env)

    yield csync

    csync.destroy()

CONTROL_ENDPOINTS = [
    ("/status", "GET"),
    ("/start", "POST"),
    ("/pause", "POST"),
    ("/resume", "POST"),
    ("/finalize", "POST"),
]

def check_external_reachability(host, path="/status", method="GET", port=2242, timeout=3):
    """
    Checks if a given endpoint is reachable externally.
    """
    try:
        requests.request(method, f"http://{host}:{port}{path}", timeout=timeout)
        return True
    except requests.exceptions.RequestException:
        return False

def check_internal_reachability(csync, host, port=2242, timeout=10, interval=0.5):
    """
    Checks if the web server is reachable internally
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = csync.container.exec_run(f"curl -s -m 2 http://{host}:{port}/status")
        if result.exit_code == 0:
            return True
        time.sleep(interval)
    return False

def start_via_host(host, port=2242, ready_timeout=30, timeout=5):
    """
    Starts replication over HTTP from outside the container
    """
    start_time = time.time()
    while time.time() - start_time < ready_timeout:
        if check_external_reachability(host, path="/status", method="GET", port=port):
            break
        time.sleep(0.5)
    else:
        Cluster.log(f"HTTP server not ready after {ready_timeout} seconds via {host}")
        return False

    try:
        response = requests.post(f"http://{host}:{port}/start", json={}, timeout=timeout)
        return response.json().get("ok") is True
    except (requests.exceptions.RequestException, ValueError):
        return False

def wait_for_repl_stage_via_host(host, port=2242, timeout=60, interval=1, stable_duration=2):
    """
    External-HTTP equivalent of Clustersync.wait_for_repl_stage(), for the same
    reason as start_via_host() above.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            data = requests.get(f"http://{host}:{port}/status", timeout=5).json()
        except (requests.exceptions.RequestException, ValueError):
            data = None
        if not data or not data.get("ok"):
            time.sleep(interval)
            continue
        initial_sync = data.get("initialSync")
        if not initial_sync or "completed" not in initial_sync:
            time.sleep(interval)
            continue
        if initial_sync["completed"]:
            stable_start = time.time()
            while time.time() - stable_start < stable_duration:
                try:
                    stable_data = requests.get(f"http://{host}:{port}/status", timeout=5).json()
                except (requests.exceptions.RequestException, ValueError):
                    return False
                if stable_data.get("state") != "running":
                    return False
                time.sleep(0.5)
            return True
        time.sleep(interval)
    return False

def wait_for_container_exit(csync, timeout=30):
    """
    Poll csync's container until it exits
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            container = csync.container
            container.reload()
            if container.status in ("exited", "dead"):
                return container.attrs["State"]["ExitCode"]
        except docker.errors.NotFound:
            return None
        time.sleep(0.5)
    return None

@pytest.mark.timeout(3600, func_only=True)
@pytest.mark.parametrize(
    "csync_env, listen_host_args, expect_start, expect_reachable, internal_check_host, use_external_control", [
    ({}, "", True, False, None, False),
    ({"PCSM_LISTEN_HOST": "0.0.0.0"}, "", True, True, None, False),
    ({}, "--listen-host=0.0.0.0", True, True, None, False),
    ({"PCSM_LISTEN_HOST": "::1"}, "", True, False, "[::1]", False),
    ({"PCSM_LISTEN_HOST": "localhost"}, "", True, False, None, False),
    ({"PCSM_LISTEN_HOST": "csync"}, "", True, True, "csync", True),
    ({"PCSM_LISTEN_HOST": "localhost:2242"}, "", False, None, None, False),
    ({"PCSM_LISTEN_HOST": "127.0.0.1:2242"}, "", False, None, None, False),
    ({"PCSM_LISTEN_HOST": "[::1]:2242"}, "", False, None, None, False),
    ({}, "--listen-host=localhost:2242", False, None, None, False),
], indirect=["csync_env"])
def test_listen_host_PCSM_345(csync, src_cluster, dst_cluster, csync_env, listen_host_args, expect_start, expect_reachable, internal_check_host, use_external_control):
    """
    Test PCSM web-server options --listen-host and PCSM_LISTEN_HOST
    """
    if listen_host_args:
        csync.create(extra_args=f"--reset-state {listen_host_args}")

    if not expect_start:
        exit_code = wait_for_container_exit(csync)
        assert exit_code is not None, "csync did not exit"
        assert exit_code != 0, "csync exited 0"
        expected_log = "listen-host must not include a port"
        assert csync.wait_for_log(expected_log), \
            f"Expected '{expected_log}' does not appear in logs for csync_env={csync_env}, listen_host_args={listen_host_args!r}: {csync.logs(tail=None)}"
        return

    if use_external_control:
        assert start_via_host(csync.name), "Failed to start csync service via external host"
        assert wait_for_repl_stage_via_host(csync.name), "Failed to reach replication stage via external host"
    else:
        assert csync.start(), "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"

    for path, method in CONTROL_ENDPOINTS:
        reachable = check_external_reachability(csync.name, path=path, method=method)
        assert reachable == expect_reachable, (
            f"Expected external reachability of {method} {path} to be {expect_reachable} with "
            f"csync_env={csync_env}, listen_host_args={listen_host_args!r}, got {reachable}"
        )

    if internal_check_host:
        assert check_internal_reachability(csync, internal_check_host), (
            f"csync's own HTTP server not reachable via {internal_check_host} internally "
            f"(csync_env={csync_env}, listen_host_args={listen_host_args!r})"
        )
    else:
        assert csync.status()["success"], "csync.status() should still work against localhost"
