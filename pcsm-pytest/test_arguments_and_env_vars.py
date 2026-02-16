import json

import pytest
import pymongo

from cluster import Cluster
from clustersync import Clustersync
from conftest import get_cluster_config
from data_generator import create_all_types_db

@pytest.fixture(scope="function")
def src_cluster():
    """Create src cluster once per function"""
    config = get_cluster_config("replicaset")
    cluster = Cluster(config['src_config'])
    cluster.create()
    yield cluster
    cluster.destroy()

@pytest.fixture(scope="function")
def dst_cluster():
    """Create dst cluster once per function"""
    config = get_cluster_config("replicaset")
    cluster = Cluster(config['dst_config'])
    cluster.create()
    yield cluster
    cluster.destroy()

@pytest.fixture(scope="function")
def csync(src_cluster, dst_cluster, request, csync_env):
    """Recreate only PCSM container with new env vars/log level for each test"""
    # Extract log level marker
    log_marker = request.node.get_closest_marker("csync_log_level")
    log_level = log_marker.args[0] if log_marker and log_marker.args else "debug"

    # Create csync instance with env vars from csync_env fixture
    csync = Clustersync('csync',
                        src_cluster.csync_connection,
                        dst_cluster.csync_connection)
    csync.create(log_level=log_level, env_vars=csync_env)

    yield csync

    # Only destroy PCSM container, not MongoDB clusters
    csync.destroy()

def cleanup_test_databases(connection):
    """Drop test databases between test runs"""
    client = pymongo.MongoClient(connection)
    for db_name in ["percona_clustersync_mongodb", "test_db"]:
        try:
            client.drop_database(db_name)
        except Exception:
            pass

def create_test_collection(connection):
    """Create a simple test collection with one document"""
    client = pymongo.MongoClient(connection)
    db = client["test_db"]
    collection = db["test_collection"]
    collection.insert_one({"test": "data"})

def check_command_output(expected_output, actual_output):
    """
    Checks if expected output is in the stdout or stderr of the command.
    """
    stdout = actual_output.cmd_stdout.strip()
    stderr = actual_output.cmd_stderr.strip()
    if expected_output in stdout or expected_output in stderr:
        return True
    raise AssertionError(
        f"Expected {expected_output!r} in command output, "
        f"got stdout={stdout!r}, stderr={stderr!r}"
    )

@pytest.mark.timeout(300, func_only=True)
def test_clone_collections_num_PML_T70(csync, src_cluster, dst_cluster):
    """
    Test PCSM --clone-num-parallel-collections and cloneNumParallelCollections argument
    """
    test_cases = [
        (["--clone-num-parallel-collections=true"], False,
         'Error: invalid argument "true" for "--clone-num-parallel-collections" flag: strconv.ParseInt: parsing "true": invalid syntax',
         "", "cli"),
        (["--clone-num-parallel-collections=1"], True, '"ok": true', "NumParallelCollections: 1", "cli"),
        (["--clone-num-parallel-collections=100"], True, '"ok": true', "NumParallelCollections: 100", "cli"),
        (["--clone-num-parallel-collections=1.5"], False,
         'Error: invalid argument "1.5" for "--clone-num-parallel-collections" flag: strconv.ParseInt: parsing "1.5": invalid syntax',
         "", "cli"),
        (["--clone-num-parallel-collections=05"], True, '"ok": true', "NumParallelCollections: 5", "cli"),
        # (["--clone-num-parallel-collections=0"], False, '', "", "cli"), # Note: Test broken due to PCSM-278
        # (["--clone-num-parallel-collections=-1"], False, '', "", "cli"), # Note: Test broken due to PCSM-278
        (["--clone-num-parallel-collections=test"], False,
         'Error: invalid argument "test" for "--clone-num-parallel-collections" flag: strconv.ParseInt: parsing "test": invalid syntax',
         "", "cli"),
        (["--clone-num-parallel-collections"], False, 'flag needs an argument: --clone-num-parallel-collections', "",
         "cli"),
        ({"cloneNumParallelCollections": 5}, True, '"ok":true', "NumParallelCollections: 5", "http"),
        # ({"cloneNumParallelCollections":-1}, False, 'Bad Request', "", "http"), # Note: Test broken due to PCSM-278
    ]
    failures = []
    create_test_collection(src_cluster.connection)
    for idx, (raw_args, should_pass, expected_cmd_return, expected_log, mode) in enumerate(test_cases):
        try:
            result = csync.start(mode=mode, raw_args=raw_args)
            assert result == should_pass, f"Expected should_pass={should_pass}, got {result}"
            assert check_command_output(expected_cmd_return, csync), \
                f"Expected command output '{expected_cmd_return}', got STDOUT: {csync.cmd_stdout} STDERR: {csync.cmd_stderr}"
            if expected_log and should_pass:
                assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"
            if should_pass:
                assert csync.wait_for_repl_stage(), "Failed to start replication stage"
                assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
        except AssertionError as e:
            failures.append(f"Case {idx+1} {raw_args}: {str(e)}")
        finally:
            cleanup_test_databases(dst_cluster.connection)
            csync.create()
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} cases:\n" + "\n".join(failures))

@pytest.mark.timeout(300, func_only=True)
def test_clone_num_read_workers_PML_T71(csync, src_cluster, dst_cluster):
    """
    Test PCSM --clone-num-read-workers and cloneNumReadWorkers argument
    """
    test_cases = [
        (["--clone-num-read-workers=true"], False,
         'Error: invalid argument "true" for "--clone-num-read-workers" flag: strconv.ParseInt: parsing "true": invalid syntax',
         "", "cli"),
        (["--clone-num-read-workers=1"], True, '"ok": true', "NumReadWorkers: 1", "cli"),
        (["--clone-num-read-workers=05"], True, '"ok": true', "NumReadWorkers: 5", "cli"),
        # (["--clone-num-read-workers=0"], False, '', "", "cli"), # Note: Test broken due to PCSM-278
        (["--clone-num-read-workers"], False, 'flag needs an argument: --clone-num-read-workers', "",
         "cli"),
        ({"cloneNumReadWorkers": 5}, True, '"ok":true', "NumReadWorkers: 5", "http"),
    ]
    failures = []
    create_test_collection(src_cluster.connection)
    for idx, (raw_args, should_pass, expected_cmd_return, expected_log, mode) in enumerate(test_cases):
        try:
            result = csync.start(mode=mode, raw_args=raw_args)
            assert result == should_pass, f"Expected should_pass={should_pass}, got {result}"
            assert check_command_output(expected_cmd_return, csync), \
                f"Expected command output '{expected_cmd_return}', got STDOUT: {csync.cmd_stdout} STDERR: {csync.cmd_stderr}"
            if expected_log and should_pass:
                assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"
            if should_pass:
                assert csync.wait_for_repl_stage(), "Failed to start replication stage"
                assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
        except AssertionError as e:
            failures.append(f"Case {idx+1} {raw_args}: {str(e)}")
        finally:
            cleanup_test_databases(dst_cluster.connection)
            csync.create()
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} cases:\n" + "\n".join(failures))

@pytest.mark.timeout(300, func_only=True)
def test_clone_num_insert_workers_PML_T72(csync, src_cluster, dst_cluster):
    """
    Test PCSM --clone-num-insert-workers and cloneNumInsertWorkers argument
    """
    test_cases = [
        (["--clone-num-insert-workers=true"], False,
         'Error: invalid argument "true" for "--clone-num-insert-workers" flag: strconv.ParseInt: parsing "true": invalid syntax',
         "", "cli"),
        (["--clone-num-insert-workers=1"], True, '"ok": true', "NumInsertWorkers: 1", "cli"),
        (["--clone-num-insert-workers=05"], True, '"ok": true', "NumInsertWorkers: 5", "cli"),
        # (["--clone-num-insert-workers=0"], False, '', "", "cli"), # Note: Test broken due to PCSM-278
        (["--clone-num-insert-workers"], False, 'flag needs an argument: --clone-num-insert-workers', "",
         "cli"),
        ({"cloneNumInsertWorkers": 5}, True, '"ok":true', "NumInsertWorkers: 5", "http"),
    ]
    failures = []
    create_test_collection(src_cluster.connection)
    for idx, (raw_args, should_pass, expected_cmd_return, expected_log, mode) in enumerate(test_cases):
        try:
            result = csync.start(mode=mode, raw_args=raw_args)
            assert result == should_pass, f"Expected should_pass={should_pass}, got {result}"
            assert check_command_output(expected_cmd_return, csync), f"Expected '{expected_cmd_return}', got STDOUT: {csync.cmd_stdout} STDERR: {csync.cmd_stderr}"
            if expected_log and should_pass:
                assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"
            if should_pass:
                assert csync.wait_for_repl_stage(), "Failed to start replication stage"
                assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
        except AssertionError as e:
            failures.append(f"Case {idx+1} {raw_args}: {str(e)}")
        finally:
            cleanup_test_databases(dst_cluster.connection)
            csync.create()
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} cases:\n" + "\n".join(failures))

@pytest.mark.timeout(300, func_only=True)
def test_clone_segment_size_PML_T73(csync, src_cluster, dst_cluster):
    """
    Test PCSM --clone-segment-size and cloneSegmentSize argument
    """
    test_cases = [
        (["--clone-segment-size=true"], False,
         'Error: invalid clone segment size: invalid cloneSegmentSize value: true: strconv.ParseFloat: parsing "": invalid syntax',
         "", "cli"),
        # Exactly 457.76MiB (true size)
        (["--clone-segment-size=479994880B"], True, '"ok": true', "SegmentSizeBytes: 479994880 (480 MB)", "cli"),
        (["--clone-segment-size=64GiB"], True, '"ok": true', "SegmentSizeBytes: 68719476736 (69 GB)", "cli"),
        ({"cloneSegmentSize": "64GiB"}, True, '"ok":true', "DBG SegmentSizeBytes: 68719476736 (69 GB)", "http"),
    ]
    failures = []
    create_test_collection(src_cluster.connection)
    for idx, (raw_args, should_pass, expected_cmd_return, expected_log, mode) in enumerate(test_cases):
        try:
            result = csync.start(mode=mode, raw_args=raw_args)
            assert result == should_pass, f"Expected should_pass={should_pass}, got {result}"
            assert check_command_output(expected_cmd_return, csync), f"Expected '{expected_cmd_return}', got STDOUT: {csync.cmd_stdout} STDERR: {csync.cmd_stderr}"
            if expected_log and should_pass:
                assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"
            if should_pass:
                assert csync.wait_for_repl_stage(), "Failed to start replication stage"
                assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
        except AssertionError as e:
            failures.append(f"Case {idx+1} {raw_args}: {str(e)}")
        finally:
            cleanup_test_databases(dst_cluster.connection)
            csync.create()
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} cases:\n" + "\n".join(failures))

@pytest.mark.timeout(300, func_only=True)
def test_clone_read_batch_size_PML_T74(csync, src_cluster, dst_cluster):
    """
    Test PCSM --clone-read-batch-size and cloneReadBatchSize argument
    """
    test_cases = [
        (["--clone-read-batch-size=true"], False,
         'Error: invalid clone read batch size: invalid cloneReadBatchSize value: true: strconv.ParseFloat: parsing "": invalid syntax',
         "", "cli"),
        (["--clone-read-batch-size=16777216B"], True, '"ok": true', "ReadBatchSizeBytes: 16777216 (17 MB)", "cli"),
        (["--clone-read-batch-size=16MiB"], True, '"ok": true', "ReadBatchSizeBytes: 16777216 (17 MB)", "cli"),
        # (["--clone-read-batch-size=2GiB"], True, '"ok": true', "", "cli"), Note: Test broken due to PCSM-278
        ({"cloneReadBatchSize":"1GiB"}, True, '"ok":true', "", "http"),
    ]
    failures = []
    create_test_collection(src_cluster.connection)
    for idx, (raw_args, should_pass, expected_cmd_return, expected_log, mode) in enumerate(test_cases):
        try:
            result = csync.start(mode=mode, raw_args=raw_args)
            assert result == should_pass, f"Expected should_pass={should_pass}, got {result}"
            assert check_command_output(expected_cmd_return, csync), f"Expected '{expected_cmd_return}', got STDOUT: {csync.cmd_stdout} STDERR: {csync.cmd_stderr}"
            if expected_log and should_pass:
                assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"
            if should_pass:
                assert csync.wait_for_repl_stage(), "Failed to start replication stage"
                assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
        except AssertionError as e:
            failures.append(f"Case {idx+1} {raw_args}: {str(e)}")
        finally:
            cleanup_test_databases(dst_cluster.connection)
            csync.create()
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} cases:\n" + "\n".join(failures))

@pytest.mark.csync_env({"PCSM_USE_COLLECTION_BULK_WRITE": "True"})
@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, expected_log, mode", [
                            ([], True, "Use collection-level bulk write", "cli"),
])
def test_pcsm_use_collection_bulk_write_env_var_PML_T76(start_cluster, src_cluster, dst_cluster, csync, raw_args, expected_log, should_pass, mode, csync_env):
    """
    Test the PCSM_USE_COLLECTION_BULK_WRITE environment variable
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(mode=mode, raw_args=raw_args) == should_pass, "Failed to start csync service"
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        if "operation_threads_2" in locals():
            all_threads += operation_threads_2
        if "operation_threads_3" in locals():
            all_threads += operation_threads_3
        for thread in all_threads:
            thread.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"


@pytest.mark.parametrize("csync_env", [
    {"PCSM_LOG_LEVEL": "DEBUG"},
    {"PCSM_LOG_LEVEL": "INFO"},
    {"PCSM_LOG_LEVEL": "WARN"},
    {"PCSM_LOG_LEVEL": "ERROR"},
], indirect=True)
@pytest.mark.parametrize("mode", [
    "cli",
])
@pytest.mark.timeout(300, func_only=True)
def test_pcsm_log_level_env_var_PML_T75(csync, src_cluster, dst_cluster, csync_env, mode):
    """
    Test the PCSM_LOG_LEVEL environment variable
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db")
        assert csync.start(mode=mode)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"

        log_level = csync_env["PCSM_LOG_LEVEL"]

        # Needed to produce ERR in logs
        csync.start(mode=mode)

        logs = csync.logs(tail=3000)

        if log_level == "DEBUG":
            assert "DBG" in csync.cmd_stderr, f"Actual log: '{csync.cmd_stderr}'"
            for log_type in ["DBG", "INF", "WRN", "ERR"]:
                assert log_type in logs, f"{log_type} not found in logs"
        elif log_level == "INFO":
            for log_type in ["INF", "WRN", "ERR"]:
                assert log_type in logs, f"{log_type} not found in logs"
            assert "DBG" not in logs, "Unexpected Debug found in logs"
        elif log_level == "WARN":
            for log_type in ["WRN", "ERR"]:
                assert log_type in logs, f"{log_type} not found in logs"
            for unexpected_log_type in ["DBG", "INF"]:
                assert unexpected_log_type not in logs, f"Unexpected '{unexpected_log_type}' found in logs"
        elif log_level == "ERROR":
            for log_type in ["ERR"]:
                assert log_type in logs, f"{log_type} not found in logs"
            for unexpected_log_type in ["DBG", "INF", "WRN"]:
                assert unexpected_log_type not in logs, f"Unexpected '{unexpected_log_type}' found in logs"
    finally:
        cleanup_test_databases(dst_cluster.connection)
        csync.create()

@pytest.mark.csync_env({"PCSM_LOG_JSON": "True"})
@pytest.mark.timeout(300, func_only=True)
def test_pcsm_log_json_env_var_PML_T76(csync, src_cluster, dst_cluster, csync_env):
    """
    Test the PCSM_LOG_JSON environment variable
    """
    create_test_collection(src_cluster.connection)
    assert csync.start()
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    assert csync.wait_for_zero_lag() is True, "Failed to catch up on replication"
    for line in csync.logs().splitlines():
        try:
            json.loads(line)
        except json.JSONDecodeError:
            raise AssertionError(
                f"Log line '{line}' is not valid JSON"
            )