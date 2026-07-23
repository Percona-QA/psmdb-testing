import pytest
import pymongo

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

def make_csync(src_cluster, dst_cluster, src_options=None, dst_options=None, extra_args="--reset-state"):
    """
    Build and start a fresh PCSM container, appending the provided
    options to the source and target connection strings
    """
    src_uri = src_cluster.csync_connection
    dst_uri = dst_cluster.csync_connection
    if src_options:
        src_uri += f"&{src_options}"
    if dst_options:
        dst_uri += f"&{dst_options}"
    csync = Clustersync('csync', src_uri, dst_uri)
    csync.create(extra_args=extra_args)
    return csync

def cleanup_test_databases(connection):
    """Drop test databases between test runs"""
    client = pymongo.MongoClient(connection)
    for db_name in ["test_db", "init_test_db"]:
        try:
            client.drop_database(db_name)
        except Exception:
            pass

@pytest.fixture(scope="function", autouse=True)
def cleanup_databases(src_cluster, dst_cluster):
    """Drop test databases on both src and dst before every test in this module"""
    cleanup_test_databases(src_cluster.connection)
    cleanup_test_databases(dst_cluster.connection)

def create_test_collection(connection):
    """Create a simple test collection with one document"""
    client = pymongo.MongoClient(connection)
    db = client["test_db"]
    collection = db["test_collection"]
    collection.insert_one({"test": "data"})


@pytest.mark.timeout(300, func_only=True)
def test_maxpoolsize_config_PML_T102(src_cluster, dst_cluster):
    """
    Verify maxPoolSize values passed via the source/target connection string
    """
    test_cases = [
        ("maxPoolSize=500", "maxPoolSize=500", ["client maxPoolSize: 500"], True),
        ("maxPoolSize=0", "maxPoolSize=0", ["client maxPoolSize: 0 (unlimited)"], True),
        ("maxPoolSize=-1", "maxPoolSize=-1", ['invalid value for "maxPoolSize": "-1"'], False),
        ("maxPoolSize=abc", "maxPoolSize=abc", ['invalid value for "maxPoolSize": "abc"'], False),
        ("maxPoolSize=7", None, ["source client maxPoolSize: 7", "target client maxPoolSize: 100 (driver default)"], True),
        (None, None, ["source client maxPoolSize: 100 (driver default)", "target client maxPoolSize: 100 (driver default)"], True),
    ]
    failures = []
    create_test_collection(src_cluster.connection)
    csync = None
    for idx, (src_options, dst_options, expected_logs, should_start) in enumerate(test_cases):
        try:
            csync = make_csync(src_cluster, dst_cluster, src_options=src_options, dst_options=dst_options)
            result = csync.start()
            assert result == should_start, f"Expected start()={should_start}, got {result}"
            if should_start:
                assert csync.wait_for_repl_stage(), "Failed to start replication stage"
            logs = csync.logs(tail=None)
            for expected_log in expected_logs:
                assert expected_log in logs, f"Expected '{expected_log}' does not appear in logs"
        except AssertionError as e:
            failures.append(f"Case {idx+1} [src={src_options or 'unset'}, dst={dst_options or 'unset'}]: {str(e)}")
        finally:
            if csync is not None:
                csync.destroy()
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} cases:\n" + "\n".join(failures))


@pytest.mark.timeout(300, func_only=True)
def test_other_pool_options_still_stripped_PML_T103(src_cluster, dst_cluster):
    """
    Verify only maxPoolSize is allowed while
    maxConnecting, maxIdleTimeMS, waitQueueMultiple, and waitQueueTimeoutMS
    display a warning
    """
    test_cases = [
        ("minPoolSize", "minPoolSize=5"),
        ("maxConnecting", "maxConnecting=5"),
        ("maxIdleTimeMS", "maxIdleTimeMS=10000"),
        ("waitQueueMultiple", "waitQueueMultiple=5"),
        ("waitQueueTimeoutMS", "waitQueueTimeoutMS=10000"),
    ]
    failures = []
    create_test_collection(src_cluster.connection)
    csync = None
    for idx, (key, options) in enumerate(test_cases):
        try:
            csync = make_csync(src_cluster, dst_cluster, src_options=options, dst_options=options)
            assert csync.start(), "Failed to start csync service"
            logs = csync.logs(tail=None)
            expected_log = f'Connection string option "{key}" is not allowed'
            assert expected_log in logs, f"Expected '{expected_log}' in logs for '{options}'"
        except AssertionError as e:
            failures.append(f"Case {idx+1} [{options}]: {str(e)}")
        finally:
            if csync is not None:
                csync.destroy()
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} cases:\n" + "\n".join(failures))
