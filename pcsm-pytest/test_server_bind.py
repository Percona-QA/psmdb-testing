import pytest
import requests
from cluster import Cluster
from clustersync import Clustersync
from conftest import get_cluster_config


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

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("csync_env, listen_host_args, expected_bind_host", [
    ({"PCSM_LISTEN_HOST": "0.0.0.0"}, "", "0.0.0.0"),
    ({}, "--listen-host=0.0.0.0", "0.0.0.0"),
    ({"PCSM_LISTEN_HOST": "localhost"}, "--listen-host=0.0.0.0", "0.0.0.0"),
], indirect=["csync_env"])
def test_listen_host_PLM_T107(csync, src_cluster, dst_cluster, csync_env, listen_host_args, expected_bind_host):
    """
    Verify PCSM server-bind options --listen-host and PCSM_LISTEN_HOST
    """
    if listen_host_args:
        csync.create(extra_args=f"--reset-state {listen_host_args}", env_vars=csync_env)

    expected_log = f"Starting HTTP server at http://{expected_bind_host}:2242"
    assert csync.wait_for_log(expected_log), (
        f"Expected '{expected_log}' does not appear in logs for csync_env={csync_env}, "
        f"listen_host_args={listen_host_args!r}: {csync.logs(tail=None)}"
    )

    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"

    for path, method in CONTROL_ENDPOINTS:
        reachable = check_external_reachability(csync.name, path=path, method=method)
        assert reachable, (
            f"Expected external reachability of {method} {path} with "
            f"csync_env={csync_env}, listen_host_args={listen_host_args!r}, got {reachable}"
        )

    assert csync.status()["success"], "csync.status() should still work against localhost"
