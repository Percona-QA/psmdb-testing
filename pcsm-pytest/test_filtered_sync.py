import pytest
import re

from data_generator import create_all_types_db, stop_all_crud_operations
from data_integrity_check import compare_data

def check_expected_logs(csync, expected_logs):
    logs = csync.logs(tail=2000)
    for pattern in expected_logs:
        if not re.search(pattern, logs):
            raise AssertionError(f"Expected log pattern not found: {pattern}")
    return True

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize(
    "include_namespaces, exclude_namespaces, skip_entries, skip_prefixes",
    [
        (["init_test_db.*", "repl_test_db.*"], [], [], ['clone_test_db']),
        (["clone_test_db.*", "repl_test_db.*"], [], [], ['init_test_db']),
        ([], ["init_test_db.*", "clone_test_db.*"], [], ['init_test_db', 'clone_test_db']),
        ([], ["clone_test_db.*", "repl_test_db.*"], [], ['clone_test_db', 'repl_test_db']),
        (["init_test_db.*"], ["init_test_db.*"], [], ['init_test_db', 'clone_test_db', 'repl_test_db']),
        (["repl_test_db.*"], ["repl_test_db.multi_key_indexes"],
                            [('repl_test_db', 'hash mismatch')], ['init_test_db', 'clone_test_db', 'repl_test_db.multi_key_indexes']),
        ([], ["init_test_db.compound_indexes"],
                            [('init_test_db', 'hash mismatch')], ['init_test_db.compound_indexes']),
    ])
def test_csync_PML_T35(start_cluster, src_cluster, dst_cluster, csync, include_namespaces, exclude_namespaces, skip_entries, skip_prefixes):
    """
    Test to check PCSM functionality with include/exclude namespaces
    """
    try:
        is_sharded = src_cluster.layout == "sharded"
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.start(include_namespaces=include_namespaces, exclude_namespaces=exclude_namespaces), "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
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
    filtered_mismatches = [
        (name, reason) for name, reason in mismatches
        if not (
            any(name.startswith(prefix) for prefix in skip_prefixes) or
            (name, reason) in skip_entries
        )]
    result = len(filtered_mismatches) == 0
    assert result is True, f"Data mismatch after synchronization: {filtered_mismatches}"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_PML_T36(start_cluster, src_cluster, dst_cluster, csync):
    """
    Test to check that PCSM correctly restores include/exclude filter after restart
    """
    try:
        is_sharded = src_cluster.layout == "sharded"
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.start(include_namespaces=["init_test_db.*", "repl_test_db.*"],
                             exclude_namespaces=["init_test_db.compound_indexes"]), "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        result = csync.wait_for_checkpoint()
        assert result is True, "Clustersync failed to save checkpoint"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
        csync.restart()
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
    skip_prefixes = ['clone_test_db','init_test_db.compound_indexes']
    skip_entries = [('init_test_db', 'hash mismatch')]
    result, mismatches = compare_data(src_cluster, dst_cluster)
    filtered_mismatches = [
        (name, reason) for name, reason in mismatches
        if not (
            any(name.startswith(prefix) for prefix in skip_prefixes) or
            (name, reason) in skip_entries
        )]
    result = len(filtered_mismatches) == 0
    assert result is True, f"Data mismatch after synchronization: {filtered_mismatches}"
    csync_error, error_logs = csync.check_csync_errors()
    expected_error = "detected concurrent process"
    if not csync_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))


@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize(
    "include_namespaces, exclude_namespaces, skip_entries, skip_prefixes, expected_logs",
    [
        # Wildcard
        # No quotes
        # Trailing commas
        ("init_test_db.*,repl_test_db.*,", "repl_test_db.wildcard_indexes", [('repl_test_db', 'hash mismatch')], ['repl_test_db.wildcard_indexes'], ["Namespace \"repl_test_db.wildcard_indexes.*\" excluded"]),
        ("", "init_test_db.*,repl_test_db.*,", [], ['init_test_db', 'repl_test_db'], ['Namespace "init_test_db.*" excluded', 'Namespace "repl_test_db.*" excluded']),

        # Include and Exclude with single quotes
        # Exclude takes priority
        ("'init_test_db.*,repl_test_db.*'", "'repl_test_db.wildcard_indexes'", [('repl_test_db', 'hash mismatch')], ['repl_test_db.wildcard_indexes'], ["Namespace \"repl_test_db.wildcard_indexes.*\" excluded"]),

        # Include and Exclude with double quotes
        ('"init_test_db.*,repl_test_db.*"', '"repl_test_db.wildcard_indexes"', [('repl_test_db', 'hash mismatch')], ['repl_test_db.wildcard_indexes'], ["Namespace \"repl_test_db.wildcard_indexes.*\" excluded"]),

        # No arguments
        (" ", " ", [], [], ['Collection "init_test_db.*" cloned', 'Collection "clone_test_db.*" cloned', 'Collection "repl_test_db.*" cloned'])

    ])
def test_csync_PML_T57(start_cluster, src_cluster, dst_cluster, csync, include_namespaces, exclude_namespaces, skip_entries, skip_prefixes, expected_logs):
    """
    Test to check PCSM CLI functionality with include/exclude namespaces
    """
    try:
        is_sharded = src_cluster.layout == "sharded"
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=is_sharded)
        assert csync.start(include_namespaces=include_namespaces, exclude_namespaces=exclude_namespaces, mode="cli"), "Failed to start csync service"
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
