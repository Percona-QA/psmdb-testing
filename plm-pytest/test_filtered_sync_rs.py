import pytest
import pymongo
import time
import docker

from cluster import Cluster
from perconalink import Perconalink
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
def plink(srcRS,dstRS):
    return Perconalink('plink',srcRS.plink_connection, dstRS.plink_connection)

@pytest.fixture(scope="module")
def start_cluster(srcRS, dstRS, plink, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        srcRS.create()
        dstRS.create()
        yield True

    finally:
        srcRS.destroy()
        dstRS.destroy()
        plink.destroy()

@pytest.fixture(scope="function")
def reset_state(srcRS, dstRS, plink, request):
    src_client = pymongo.MongoClient(srcRS.connection)
    dst_client = pymongo.MongoClient(dstRS.connection)
    def print_logs():
        if request.config.getoption("--verbose"):
            logs = plink.logs()
            print(f"\n\nplink Last 50 Logs for plink:\n{logs}\n\n")
    request.addfinalizer(print_logs)
    plink.destroy()
    for db_name in src_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            src_client.drop_database(db_name)
    for db_name in dst_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            dst_client.drop_database(db_name)
    plink.create()

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
def test_rs_plink_PML_T35(reset_state, srcRS, dstRS, plink, include_namespaces, exclude_namespaces, skip_entries, skip_prefixes):
    """
    Test to check PLM functionality with include/exclude namespaces
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = plink.start(include_namespaces=include_namespaces, exclude_namespaces=exclude_namespaces)
        assert result is True, "Failed to start plink service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = plink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        time.sleep(5)
    except Exception as e:
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
    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"
    result, mismatches = compare_data_rs(srcRS, dstRS)
    filtered_mismatches = [
        (name, reason) for name, reason in mismatches
        if not (
            any(name.startswith(prefix) for prefix in skip_prefixes) or
            (name, reason) in skip_entries
        )]
    result = len(filtered_mismatches) == 0
    assert result is True, f"Data mismatch after synchronization: {filtered_mismatches}"
    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors in logs: {error_logs}"

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_plink_PML_T36(reset_state, srcRS, dstRS, plink):
    """
    Test to check that PLM correctly restores include/exclude filter after restart
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = plink.start(include_namespaces=["init_test_db.*", "repl_test_db.*"],
                             exclude_namespaces=["init_test_db.compound_indexes"])
        assert result is True, "Failed to start plink service"
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = plink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        result = plink.wait_for_checkpoint()
        assert result is True, "Perconalink failed to save checkpoint"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        plink.restart()
        time.sleep(5)
    except Exception as e:
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
    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"
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
    plink_error, error_logs = plink.check_plink_errors()
    expected_error = "detected concurrent process"
    if not plink_error:
        unexpected = [line for line in error_logs if expected_error not in line]
        if unexpected:
            pytest.fail("Unexpected error(s) in logs:\n" + "\n".join(unexpected))