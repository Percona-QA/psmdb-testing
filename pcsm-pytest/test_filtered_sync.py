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
            try:
                assert expected_mismatch in mismatch_set, \
                    f"Expected mismatch {expected_mismatch} not found in actual mismatches: {mismatches}"
            except AssertionError:
                pytest.xfail("Known limitation: PCSM-230")
    # Filter out sharded-only collection prefixes for replica sets
    prefixes_to_check = skip_prefixes
    if not src_cluster.is_sharded:
        prefixes_to_check = [prefix for prefix in skip_prefixes if ".sharded_" not in prefix]
    for prefix in prefixes_to_check:
        matching_mismatches = [name for name, _ in mismatches if name.startswith(prefix)]
        try:
            assert len(matching_mismatches) > 0, \
                f"Expected at least one mismatch matching prefix '{prefix}', but found none. Actual mismatches: {mismatches}"
        except AssertionError:
            pytest.xfail("Known limitation: PCSM-230")

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize(
    "include_namespaces, exclude_namespaces, skip_entries, skip_prefixes",
    [
        # Test basic include functionality: include two databases
        (["init_test_db.*", "repl_test_db.*"], [], [], ['clone_test_db']),
        # Test basic exclude functionality: exclude two databases
        ([], ["clone_test_db.*", "repl_test_db.*"], [], ['clone_test_db', 'repl_test_db']),
        # Test conflict resolution: include and exclude same namespace (exclude should take priority)
        (["init_test_db.*"], ["init_test_db.*"], [], ['init_test_db', 'clone_test_db', 'repl_test_db']),
        # Test exclude multiple regular and sharded collections without include filter
        ([], ["init_test_db.geo_indexes", "init_test_db.sharded_range_key_collection", "clone_test_db.sharded_compound_key_collection"],
                            [('init_test_db', 'hash mismatch')],
                            ['init_test_db.geo_indexes', 'init_test_db.sharded_range_key_collection', 'clone_test_db.sharded_compound_key_collection']),
        # Test include single specific collections: 1 regular + 1 sharded
        (["test_include_db.regular_test_collection", "test_include_db.sharded_test_collection"], [],
                            [], ['init_test_db', 'clone_test_db', 'repl_test_db']),
        # Test exclude from multiple included namespaces
        (["init_test_db.*", "repl_test_db.*"], ["init_test_db.geo_indexes", "repl_test_db.sharded_range_key_collection"],
                            [('init_test_db', 'hash mismatch')],
                            ['clone_test_db', 'init_test_db.geo_indexes', 'repl_test_db.sharded_range_key_collection']),
    ])
def test_csync_PML_T35(start_cluster, src_cluster, dst_cluster, csync, include_namespaces, exclude_namespaces, skip_entries, skip_prefixes):
    """
    Test to check PCSM functionality with include/exclude namespaces
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(include_namespaces=include_namespaces, exclude_namespaces=exclude_namespaces), "Failed to start csync service"
        if "test_include_db" in str(include_namespaces):
            src = pymongo.MongoClient(src_cluster.connection)
            test_db = src["test_include_db"]
            test_db["regular_test_collection"].insert_many([{"_id": i, "name": f"item_{i}", "value": i * 10} for i in range(10)])
            if src_cluster.is_sharded:
                src.admin.command("enableSharding", "test_include_db")
                src.admin.command("shardCollection", "test_include_db.sharded_test_collection", key={"_id": "hashed"})
            test_db["sharded_test_collection"].insert_many([{"_id": i, "name": f"sharded_item_{i}", "value": i * 20} for i in range(10)])
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
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
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.start(include_namespaces=["init_test_db.*", "repl_test_db.*"],
                             exclude_namespaces=["init_test_db.compound_indexes"]), "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        result = csync.wait_for_checkpoint()
        assert result is True, "Clustersync failed to save checkpoint"
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
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
    csync.restart()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    skip_prefixes = ['clone_test_db','init_test_db.compound_indexes']
    skip_entries = [('init_test_db', 'hash mismatch')]
    result, mismatches = compare_data(src_cluster, dst_cluster)
    verify_expected_mismatches(src_cluster, mismatches, skip_entries, skip_prefixes)
    # Filter out expected mismatches and check for unexpected ones
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
        ("init_test_db.*,repl_test_db.*,", "repl_test_db.wildcard_indexes", [('repl_test_db', 'hash mismatch')], ['clone_test_db', 'repl_test_db.wildcard_indexes'], ["Namespace \"repl_test_db.wildcard_indexes.*\" excluded"]),
        ("", "init_test_db.*,repl_test_db.*,", [], ['init_test_db', 'repl_test_db'], ['Namespace "init_test_db\\..*" excluded', 'Namespace "repl_test_db\\..*" excluded']),

        # Include and Exclude with single quotes
        # Exclude takes priority
        ("'init_test_db.*,repl_test_db.*'", "'repl_test_db.wildcard_indexes'", [('repl_test_db', 'hash mismatch')], ['clone_test_db', 'repl_test_db.wildcard_indexes'], ["Namespace \"repl_test_db.wildcard_indexes.*\" excluded"]),

        # Include and Exclude with double quotes
        ('"init_test_db.*,repl_test_db.*"', '"repl_test_db.wildcard_indexes"', [('repl_test_db', 'hash mismatch')], ['clone_test_db', 'repl_test_db.wildcard_indexes'], ["Namespace \"repl_test_db.wildcard_indexes.*\" excluded"]),

        # No arguments - nothing will be synced cause if no values are provided, nothing should be included or excluded
        (" ", " ", [], ['init_test_db', 'clone_test_db', 'repl_test_db'], ['Namespace "init_test_db\\..*" excluded', 'Namespace "clone_test_db\\..*" excluded', 'Namespace "repl_test_db\\..*" excluded'])

    ])
def test_csync_PML_T57(start_cluster, src_cluster, dst_cluster, csync, include_namespaces, exclude_namespaces, skip_entries, skip_prefixes, expected_logs):
    """
    Test to check PCSM CLI functionality with include/exclude namespaces
    """
    try:
        _, operation_threads_1 = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_2 = create_all_types_db(src_cluster.connection, "clone_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        _, operation_threads_3 = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True, is_sharded=src_cluster.is_sharded)
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
