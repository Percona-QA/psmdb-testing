import pytest
import threading
import docker

from cluster import Cluster
from clustersync import Clustersync

pytest_plugins = ["metrics_collector"]

def pytest_addoption(parser):
    parser.addoption("--jenkins", action="store_true", default=False, help="Run tests marked as jenkins")

def pytest_collection_modifyitems(config, items):
    if config.getoption("--jenkins"):
        return
    skip_jenkins = pytest.mark.skip(reason="Skipped because --jenkins flag not set")
    for item in items:
        if "jenkins" in item.keywords:
            item.add_marker(skip_jenkins)

def cleanup_all_test_containers():
    """
    Cleanup all test containers regardless of configuration
    """
    try:
        docker_client = docker.from_env()
        all_containers = docker_client.containers.list(all=True)
        test_patterns = ['rs', 'mongos', 'rscfg', 'csync']
        for container in all_containers:
            container_name = container.name
            if any(container_name.startswith(pattern) for pattern in test_patterns):
                try:
                    container.remove(v=True, force=True)
                    Cluster.log(f"Cleaned up leftover container: {container_name}")
                except docker.errors.NotFound:
                    pass
                except Exception as e:
                    Cluster.log(f"Warning: Failed to remove container {container_name}: {e}")
    except Exception as e:
        Cluster.log(f"Warning: Error during test container cleanup: {e}")

def get_cluster_config(setup_type):
    """
    Returns configuration for the given setup type
    """
    if setup_type == "replicaset":
        return {
            "src_config": {"_id": "rs1", "members": [{"host": "rs101"}]},
            "dst_config": {"_id": "rs2", "members": [{"host": "rs201"}]},
            "layout": "replicaset"
        }
    elif setup_type == "replicaset_3n":
        return {
            "src_config": {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
            "dst_config": {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}]},
            "layout": "replicaset"
        }
    elif setup_type == "sharded":
        return {
            "src_config": {
                "mongos": "mongos1",
                "configserver": {"_id": "rscfg1", "members": [{"host": "rscfg101"}]},
                "shards": [
                    {"_id": "rs1", "members": [{"host": "rs101"}]},
                    {"_id": "rs2", "members": [{"host": "rs201"}]}
                ]
            },
            "dst_config": {
                "mongos": "mongos2",
                "configserver": {"_id": "rscfg2", "members": [{"host": "rscfg201"}]},
                "shards": [
                    {"_id": "rs3", "members": [{"host": "rs301"}]},
                    {"_id": "rs4", "members": [{"host": "rs401"}]}
                ]
            },
            "layout": "sharded"
        }
    elif setup_type == "sharded_3n":
        return {
            "src_config": {
                "mongos": "mongos1",
                "configserver": {"_id": "rscfg1", "members": [{"host": "rscfg101"}, {"host": "rscfg102"}, {"host": "rscfg103"}]},
                "shards": [
                    {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
                    {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}]}
                ]
            },
            "dst_config": {
                "mongos": "mongos2",
                "configserver": {"_id": "rscfg2", "members": [{"host": "rscfg201"}, {"host": "rscfg202"}, {"host": "rscfg203"}]},
                "shards": [
                    {"_id": "rs3", "members": [{"host": "rs301"}, {"host": "rs302"}, {"host": "rs303"}]},
                    {"_id": "rs4", "members": [{"host": "rs401"}, {"host": "rs402"}, {"host": "rs403"}]}
                ]
            },
            "layout": "sharded"
        }
    else:
        raise ValueError(f"Unknown setup type: {setup_type}")

@pytest.fixture(scope="module")
def cluster_configs(request):
    """
    Fixture that provides configuration, it must be parametrized using
    @pytest.mark.parametrize("cluster_configs", [...], indirect=True)
    """
    if not hasattr(request, "param"):
        raise ValueError(
            f"Test {request.node.name} uses cluster_configs but it's not parametrized")
    return get_cluster_config(request.param)

@pytest.fixture(scope="module")
def src_cluster(cluster_configs):
    return Cluster(cluster_configs["src_config"])

@pytest.fixture(scope="module")
def dst_cluster(cluster_configs):
    return Cluster(cluster_configs["dst_config"])

@pytest.fixture(scope="module")
def csync(src_cluster, dst_cluster):
    return Clustersync('csync', src_cluster.csync_connection, dst_cluster.csync_connection)

@pytest.fixture(scope="function")
def start_cluster(src_cluster, dst_cluster, csync, request):
    """
    Unified cluster startup fixture that works with both RS and sharded
    clusters and handles cluster creation, csync startup and cleanup
    """
    CLUSTER_CREATE_TIMEOUT = 60
    exceptions = {}
    log_marker = request.node.get_closest_marker("csync_log_level")
    log_level = log_marker.args[0] if log_marker and log_marker.args else "debug"
    env_marker = request.node.get_closest_marker("csync_env")
    env_vars = env_marker.args[0] if env_marker and env_marker.args else None
    cleanup_all_test_containers()
    def create_cluster(cluster_name, cluster):
        try:
            cluster.create()
        except Exception as e:
            Cluster.log(f"{cluster_name} cluster creation failed: {e}")
            exceptions[cluster_name] = e
    try:
        # Lambda wraps create_cluster with arguments since threading.Thread expects a zero-argument callable
        src_create_thread = threading.Thread(target=lambda: create_cluster("src", src_cluster))
        dst_create_thread = threading.Thread(target=lambda: create_cluster("dst", dst_cluster))
        src_create_thread.start()
        dst_create_thread.start()
        def wait_for_thread(thread, cluster_name):
            thread.join(timeout=CLUSTER_CREATE_TIMEOUT)
            if thread.is_alive():
                raise TimeoutError(f"{cluster_name} cluster creation timed out after {CLUSTER_CREATE_TIMEOUT} seconds")
            if cluster_name in exceptions:
                raise exceptions[cluster_name]
        try:
            wait_for_thread(src_create_thread, "src")
            wait_for_thread(dst_create_thread, "dst")
        except TimeoutError:
            raise
        csync.create(log_level=log_level, env_vars=env_vars)
        yield True
    finally:
        if request.config.getoption("--verbose"):
            logs = csync.logs()
            print(f"\n\ncsync Last 50 Logs for csync:\n{logs}\n\n")
        try:
            src_cluster.destroy()
            dst_cluster.destroy()
            csync.destroy()
        except Exception:
            cleanup_all_test_containers()