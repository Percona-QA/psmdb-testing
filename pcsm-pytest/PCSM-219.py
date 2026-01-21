import pytest
import re
import pymongo

from data_generator import create_all_types_db, stop_all_crud_operations
from data_integrity_check import compare_data

def check_expected_logs(csync, expected_logs):
    logs = csync.logs(tail=2000)
    for pattern in expected_logs:
        if not re.search(pattern, logs):
            raise AssertionError(f"Expected log pattern not found: {pattern}")
    return True

def verify_expected_mismatches(src_cluster, mismatches, skip_entries, skip_prefixes):
    """Verify that expected mismatches are actually present in the results."""
    # Verify that all expected mismatches from skip_entries are actually present
    # Expect hash mismatch only for RS setup
    if not src_cluster.is_sharded:
        mismatch_set = set(mismatches)
        for expected_mismatch in skip_entries:
            assert expected_mismatch in mismatch_set, \
                    f"Expected mismatch {expected_mismatch} not found in actual mismatches: {mismatches}"
    # Filter out sharded-only collection prefixes for replica sets
    prefixes_to_check = skip_prefixes
    if not src_cluster.is_sharded:
        prefixes_to_check = [prefix for prefix in skip_prefixes if ".sharded_" not in prefix]
    for prefix in prefixes_to_check:
        matching_mismatches = [name for name, _ in mismatches if name.startswith(prefix)]
        assert len(matching_mismatches) > 0, \
                f"Expected at least one mismatch matching prefix '{prefix}', but found none. Actual mismatches: {mismatches}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("--clone-num-parallel-collections", ["10"])
def test(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check PCSM CLI functionality with include/exclude namespaces
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(clone_num_parallel_collections=True, mode="cli"), "Failed to start csync service"
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
    result, mismatches = compare_data(src_cluster, dst_cluster)
    verify_expected_mismatches(src_cluster, mismatches, skip_entries, skip_prefixes)
    # Filter out expected mismatches and check for unexpected ones
    filtered_mismatches = [
        (name, reason) for name, reason in mismatches
        if not (
                (any(name.startswith(prefix) for prefix in skip_prefixes)) or
                (name, reason) in skip_entries
        )]
    result = len(filtered_mismatches) == 0
    assert result is True, f"Data mismatch after synchronization: {filtered_mismatches}"
    print(csync.logs())
    assert check_expected_logs(csync, expected_logs)
    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "detected concurrent process"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))