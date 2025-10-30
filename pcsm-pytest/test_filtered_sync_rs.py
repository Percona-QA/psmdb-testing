import pytest
import pymongo
import docker
import re
import threading

from cluster import Cluster
from clustersync import Clustersync
from data_generator import create_all_types_db, stop_all_crud_operations
from data_integrity_check import compare_data_rs

@pytest.fixture(scope="module")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="module")
def dstRS():
    return Cluster({ "_id": "rs2", "members": [{"host":"rs201"}]})

@pytest.fixture(scope="module")
def srcRS():
    return Cluster({ "_id": "rs1", "members": [{"host":"rs101"}]})

@pytest.fixture(scope="module")
def csync(srcRS,dstRS):
    return Clustersync('csync',srcRS.csync_connection, dstRS.csync_connection)

@pytest.fixture(scope="module")
def start_cluster(srcRS, dstRS, csync, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        src_create_thread = threading.Thread(target=srcRS.create)
        dst_create_thread = threading.Thread(target=dstRS.create)
        src_create_thread.start()
        dst_create_thread.start()
        src_create_thread.join()
        dst_create_thread.join()
        yield True

    finally:
        srcRS.destroy()
        dstRS.destroy()
        csync.destroy()

@pytest.fixture(scope="function")
def reset_state(srcRS, dstRS, csync, request):
    src_client = pymongo.MongoClient(srcRS.connection)
    dst_client = pymongo.MongoClient(dstRS.connection)
    def print_logs():
        if request.config.getoption("--verbose"):
            logs = csync.logs()
            print(f"\n\ncsync Last 50 Logs for csync:\n{logs}\n\n")
    request.addfinalizer(print_logs)
    csync.destroy()
    for db_name in src_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            src_client.drop_database(db_name)
    for db_name in dst_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            dst_client.drop_database(db_name)
    csync.create()

def check_expected_logs(csync, expected_logs):
    logs = csync.logs(tail=1000)
    for pattern in expected_logs:
        if not re.search(pattern, logs):
            raise AssertionError(f"Expected log pattern not found: {pattern}")
    return True

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
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
def test_rs_csync_PML_T35(reset_state, srcRS, dstRS, csync, include_namespaces, exclude_namespaces, skip_entries, skip_prefixes):
    """
    Test to check PCSM functionality with include/exclude namespaces
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = csync.start(include_namespaces=include_namespaces, exclude_namespaces=exclude_namespaces)
        assert result is True, "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
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
    result = csync.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"
    result, mismatches = compare_data_rs(srcRS, dstRS)
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

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_csync_PML_T36(reset_state, srcRS, dstRS, csync):
    """
    Test to check that PCSM correctly restores include/exclude filter after restart
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = csync.start(include_namespaces=["init_test_db.*", "repl_test_db.*"],
                             exclude_namespaces=["init_test_db.compound_indexes"])
        assert result is True, "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = csync.wait_for_checkpoint()
        assert result is True, "Clustersync failed to save checkpoint"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
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
    result = csync.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"
    skip_prefixes = ['clone_test_db','init_test_db.compound_indexes']
    skip_entries = [('init_test_db', 'hash mismatch')]
    result, mismatches = compare_data_rs(srcRS, dstRS)
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


@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
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
def test_rs_csync_PML_T57(reset_state, srcRS, dstRS, csync, include_namespaces, exclude_namespaces, skip_entries, skip_prefixes, expected_logs):
    """
    Test to check PCSM CLI functionality with include/exclude namespaces
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        result = csync.start(include_namespaces=include_namespaces, exclude_namespaces=exclude_namespaces, mode="cli")
        assert result is True, "Failed to start csync service"
        result = csync.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
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
    result = csync.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = csync.finalize()
    assert result is True, "Failed to finalize csync service"
    result, mismatches = compare_data_rs(srcRS, dstRS)
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