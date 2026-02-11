import json
import time

import pytest

from data_generator import create_all_types_db, stop_all_crud_operations

def check_command_output(expected_output, actual_output, is_stdout):
    if is_stdout:
        return expected_output in actual_output.cmd_stdout.strip(), f"Expected {expected_output} got {actual_output.cmd_stdout.strip()}"
    else:
        return expected_output in actual_output.cmd_stderr.strip(), f"Expected {expected_output} got {actual_output.cmd_stderr.strip()}"



@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, expected_cmd_return, expected_log, mode", [
                            (["--clone-num-parallel-collections=5"], True, '"ok": true', "NumParallelCollections: 5", "cli"),
                            # (["--clone-num-parallel-collections=-1"], False, '"ok": true', "", "cli"), Note: Test broken due to PCSM-278
                            (["--clone-num-parallel-collections=test"], False, 'Error: invalid argument "test" for "--clone-num-parallel-collections" flag: strconv.ParseInt: parsing "test": invalid syntax', "", "cli"),
                            (["--clone-num-parallel-collections"], False, 'flag needs an argument: --clone-num-parallel-collections', "", "cli"),
                            ({"cloneNumParallelCollections":5}, True, '"ok":true', "NumParallelCollections: 5", "http"),
                            # ({"cloneNumParallelCollections":-1}, False, 'Bad Request', "", "http"), Note: Test broken due to PCSM-278
])
def test_clone_collections_num_PML_T70(start_cluster, src_cluster, dst_cluster, csync, raw_args, should_pass, expected_cmd_return, expected_log, mode):
    """
    Test PCSM --clone-num-parallel-collections argument and cloneNumParallelCollections environment variable
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(mode=mode, raw_args=raw_args) == should_pass, "Failed to start csync service"
        if should_pass:
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
    if should_pass:
        assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
        assert csync.finalize(), "Failed to finalize csync service"
    assert check_command_output(expected_cmd_return, csync, should_pass)
    assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, expected_cmd_return, expected_log, mode", [
                            (["--clone-num-read-workers=5"], True, '"ok": true', "NumReadWorkers: 5", "cli"),
                            (["--clone-num-read-workers=test"], False, 'Error: invalid argument "test" for "--clone-num-read-workers" flag: strconv.ParseInt: parsing "test": invalid syntax', "", "cli"),
                            # (["--clone-num-read-workers=-1"], False, '', "", "cli"), Note: Test broken due to PCSM-278
                            ({"cloneNumReadWorkers":5}, True, '"ok":true', "NumReadWorkers: 5", "http"),
                            # ({"cloneNumReadWorkers":-1}, False, 'Bad Request', "", "http"), Note: Test broken due to PCSM-278
])
def test_clone_num_read_workers_PML_T71(start_cluster, src_cluster, dst_cluster, csync, raw_args, should_pass, expected_cmd_return, expected_log, mode):
    """
    Test PCSM --clone-num-read-workers argument and cloneNumReadWorkers environment variable
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(mode=mode, raw_args=raw_args) == should_pass, "Failed to start csync service"
        if should_pass:
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
    if should_pass:
        assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
        assert csync.finalize(), "Failed to finalize csync service"
    assert check_command_output(expected_cmd_return, csync, should_pass)
    assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, expected_cmd_return, expected_log, mode", [
                            (["--clone-num-insert-workers=5"], True, '"ok": true', "NumInsertWorkers: 5", "cli"),
                            # (["--clone-num-insert-workers=-1"], False, '', "", "cli"), Note: Test broken due to PCSM-278
                            (["--clone-num-insert-workers=test"], False, 'Error: invalid argument "test" for "--clone-num-insert-workers" flag: strconv.ParseInt: parsing "test": invalid syntax', "", "cli"),
                            ({"cloneNumInsertWorkers":5}, True, '"ok":true', "NumInsertWorkers: 5", "http"),
                            # ({"cloneNumInsertWorkers":-1}, False, '', "", "http"), Note: Test broken due to PCSM-278
])
def test_clone_num_insert_workers_PML_T72(start_cluster, src_cluster, dst_cluster, csync, raw_args, should_pass, expected_cmd_return, expected_log, mode):
    """
    Test PCSM --clone-num-insert-workers argument and cloneNumInsertWorkers environment variable
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(mode=mode, raw_args=raw_args) == should_pass, "Failed to start csync service"
        if should_pass:
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
    if should_pass:
        assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
        assert csync.finalize(), "Failed to finalize csync service"
    assert check_command_output(expected_cmd_return, csync, should_pass)
    assert expected_log in csync.logs(tail=3000), f"Expected {expected_log} does not appear in logs"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, expected_cmd_return, mode", [
                            (["--clone-segment-size=479994880"], True, '"ok": true', "cli"),
                            (["--clone-segment-size=479994880B"], True, '"ok": true', "cli"),
                            (["--clone-segment-size=test"], False, 'invalid clone segment size: invalid cloneSegmentSize value: test: strconv.ParseFloat: parsing \\"\\": invalid syntax', "cli"),
                            (["--clone-segment-size=480MB"], True, '"ok": true', "cli"),
                            (["--clone-segment-size=64GB"], True, '"ok": true', "cli"),
                            (["--clone-segment-size=64GiB"], True, '"ok": true', "cli"),
                            (["--clone-segment-size=68719476736B"], True, '"ok": true', "cli"),
                            ({"cloneSegmentSize":"64GiB"}, True, '"ok":true', "http"),
                            ({"cloneSegmentSize":"479994879B"}, False, 'invalid clone segment size: cloneSegmentSize must be at least 458 MiB, got 458 MiB', "http"),
])
def test_clone_segment_size_PML_T73(start_cluster, src_cluster, dst_cluster, csync, raw_args, should_pass, expected_cmd_return, mode):
    """
    Test PCSM --clone-segment-size argument and cloneSegmentSize environment variable
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(mode=mode, raw_args=raw_args) == should_pass, "Failed to start csync service"
        if should_pass:
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
    if should_pass:
        assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
        assert csync.finalize(), "Failed to finalize csync service"
    assert check_command_output(expected_cmd_return, csync, should_pass)

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, expected_cmd_return, expected_log, mode", [
                            (["--clone-read-batch-size=16777216"], True, '"ok": true', "ReadBatchSizeBytes: 16777216 (17 MB)", "cli"),
                            (["--clone-read-batch-size=test"], False, 'invalid clone read batch size: invalid cloneReadBatchSize value: test: strconv.ParseFloat: parsing \\"\\": invalid syntax', "", "cli"),
                            (["--clone-read-batch-size=16777216B"], True, '"ok": true', "", "cli"),
                            (["--clone-read-batch-size=16MB"], False, 'invalid clone read batch size: cloneReadBatchSize must be at least 16 MiB, got 15 MiB', "", "cli"),
                            # (["--clone-read-batch-size=2GiB"], True, '"ok": true', "", "cli"), Note: Test broken due to PCSM-278
                            # ({"cloneReadBatchSize":"2GiB"}, True, '"ok":true', "", "http"), Note: Test broken due to PCSM-278
                            ({"cloneReadBatchSize":"16777215B"}, False, 'Expected Bad Request got {"ok":false,"error":"invalid clone read batch size: cloneReadBatchSize must be at least 16 MiB, got 16 MiB"}', "", "http"),
])
def test_clone_read_batch_size_PML_T74(start_cluster, src_cluster, dst_cluster, csync, raw_args, should_pass, expected_cmd_return, expected_log, mode):
    """
    Test PCSM --clone-read-batch-size argument and cloneReadBatchSize environment variable
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(mode=mode, raw_args=raw_args) == should_pass, "Failed to start csync service"
        if should_pass:
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
    if should_pass:
        assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
        assert csync.finalize(), "Failed to finalize csync service"
    assert check_command_output(expected_cmd_return, csync, should_pass)
    assert expected_log in csync.logs(tail=3000), f"Expected {expected_log} does not appear in logs"

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, expected_cmd_return, expected_log, mode", [
                            (["--use-collection-bulk-write=True"], True, '"ok": true', "Use collection-level bulk write", "cli"),
                            (["--use-collection-bulk-write=False"], True, '"ok": true', "", "cli"),
                            (["--use-collection-bulk-write true"], True, '"ok": true', "Use collection-level bulk write", "cli"),
                            (["--use-collection-bulk-write false"], True, '"ok": true', "", "cli"),
                            (["--use-collection-bulk-write"], True, '"ok": true', "Use collection-level bulk write", "cli"),
                            (["--use-collection-bulk-write", "--use-collection-bulk-write=false"], True, '"ok": true', "", "cli"),
                            (["--use-collection-bulk-write=false", "--use-collection-bulk-write"], True, '"ok": true', "Use collection-level bulk write", "cli"),
                            (["--use-collection-bulk-write= "], False, 'Error: invalid argument "" for "--use-collection-bulk-write" flag: strconv.ParseBool: parsing "": invalid syntax', "", "cli"),
                            (["--use-collection-bulk-write==true"], False, 'Error: invalid argument "=true" for "--use-collection-bulk-write" flag: strconv.ParseBool: parsing "=true": invalid syntax', "", "cli"),
                            (["--use-collection-bulk-write=f"], True, '"ok": true', "", "cli"),
                            (["--use-collection-bulk-write=test"], False, 'Error: invalid argument "test" for "--use-collection-bulk-write" flag: strconv.ParseBool: parsing "test": invalid syntax', "", "cli"),
                            ({"useCollectionBulkWrite": True}, True, '"ok": true', "Use collection-level bulk write", "http"),
                            ({"useCollectionBulkWrite": 123}, False, 'Error: invalid argument "123" for "--use-collection-bulk-write" flag: strconv.ParseBool: parsing "123": invalid syntax', "", "http"),
])
def test_use_collection_bulk_write_PML_T70(start_cluster, src_cluster, dst_cluster, csync, raw_args, should_pass, expected_cmd_return, expected_log, mode):
    """
    Test PCSM --use-collection-bulk-write argument and useCollectionBulkWrite environment variable
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(mode=mode, raw_args=raw_args) == should_pass, "Failed to start csync service"
        if should_pass:
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
    if should_pass:
        assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
        assert csync.finalize(), "Failed to finalize csync service"
    assert check_command_output(expected_cmd_return, csync, should_pass)
    assert expected_log in csync.logs(tail=3000), f"Expected '{expected_log}' does not appear in logs"

@pytest.mark.parametrize("csync_env", [
    {"PCSM_LOG_LEVEL": "DEBUG"},
    {"PCSM_LOG_LEVEL": "INFO"},
    {"PCSM_LOG_LEVEL": "WARN"},
    {"PCSM_LOG_LEVEL": "ERROR"},
], indirect=True)
@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, mode", [
                            (["--log-json"], True, "cli"),
])
def test_pcsm_log_level_env_var_PML_T75(start_cluster, src_cluster, dst_cluster, csync, raw_args, should_pass, mode, csync_env):
    """
    Test the PCSM_LOG_LEVEL environment variable
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

    log_level = csync_env["PCSM_LOG_LEVEL"]

    if log_level == "DEBUG":
        print(f"TEST: {csync.cmd_stderr}")
        assert "debug" in csync.cmd_stderr, f"Actual log: '{csync.cmd_stderr}'"

    # Needed to produce ERR in logs
    csync.start(mode=mode, raw_args=raw_args)

    if log_level == "DEBUG":
        for log_level in ["DBG", "INF", "WRN", "ERR"]:
            assert log_level in csync.logs(), f"{log_level} not found in logs"
        assert "fatal" in csync.cmd_stderr, f"Actual log: {csync.cmd_stderr}"

    elif log_level == "INFO":
        for log_level in ["INF", "WRN", "ERR"]:
            assert log_level in csync.logs(), f"{log_level} not found in logs"
        assert "DBG" not in csync.logs(), "Unexpected Debug found in logs"
        assert "fatal" in csync.cmd_stderr, f"Actual log: {csync.cmd_stderr}"

    elif log_level == "WARN":
        for log_level in ["WRN", "ERR"]:
            assert log_level in csync.logs(), f"{log_level} not found in logs"
        for unexpected_log_level in ["DBG", "INF"]:
            assert unexpected_log_level not in csync.logs(), f"Unexpected '{unexpected_log_level}' found in logs"
        assert "fatal" in csync.cmd_stderr, f"Actual log: '{csync.cmd_stderr}'"

    elif log_level == "ERROR":
        for log_level in ["ERR"]:
            assert log_level in csync.logs(), f"{log_level} not found in logs"
        for unexpected_log_level in ["DBG", "INF", "WRN"]:
            assert unexpected_log_level not in csync.logs(), f"Unexpected '{unexpected_log_level}' found in logs"
        assert "fatal" in csync.cmd_stderr, f"Actual log: '{csync.cmd_stderr}'"

@pytest.mark.csync_env({"PCSM_LOG_JSON": "True"})
@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(2700, func_only=True)
@pytest.mark.parametrize("raw_args, should_pass, mode", [
                            ([], True, "cli"),
])
def test_pcsm_log_json_env_var_PML_T76(start_cluster, src_cluster, dst_cluster, csync, raw_args, should_pass, mode, csync_env):
    """
    Test the PCSM_LOG_JSON environment variable
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

    for line in csync.logs(tail=3000).splitlines():
        try:
            json.loads(line)
        except json.JSONDecodeError:
            raise AssertionError(
                f"Log line '{line}' is not valid JSON"
            )

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
