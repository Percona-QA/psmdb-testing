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

def assert_metrics(metrics):
    assert isinstance(metrics, dict)
    expected_metrics_with_checks = {
        'go_gc_duration_seconds{quantile="0"}': lambda v: 0 <= v <= 0.02,
        'go_gc_duration_seconds{quantile="0.25"}': lambda v: 0 <= v <= 0.02,
        'go_gc_duration_seconds{quantile="0.5"}': lambda v: 0 <= v <= 0.02,
        'go_gc_duration_seconds{quantile="0.75"}': lambda v: 0 <= v <= 0.02,
        'go_gc_duration_seconds{quantile="1"}': lambda v: 0 <= v <= 0.02,
        'go_gc_duration_seconds_sum': lambda v: 0 <= v <= 1.0,
        'go_gc_duration_seconds_count': lambda v: 0 <= v <= 10_000,
        'go_gc_gogc_percent': lambda v: v == 100,
        'go_gc_gomemlimit_bytes': lambda v: 0 < v <= 2**63,
        'go_goroutines': lambda v: 0 < v <= 100,
        'go_sched_gomaxprocs_threads': lambda v: 1 <= v <= 128,
        'go_threads': lambda v: 1 <= v <= 100,
        'go_memstats_alloc_bytes': lambda v: 0 < v <= 2**30,
        'go_memstats_alloc_bytes_total': lambda v: 0 < v <= 2**34,
        'go_memstats_buck_hash_sys_bytes': lambda v: 0 <= v <= 1_000_000,
        'go_memstats_frees_total': lambda v: 0 <= v <= 1e9,
        'go_memstats_gc_sys_bytes': lambda v: 0 <= v <= 20_000_000,
        'go_memstats_heap_alloc_bytes': lambda v: 0 < v <= 2**30,
        'go_memstats_heap_idle_bytes': lambda v: 0 <= v <= 2**34,
        'go_memstats_heap_inuse_bytes': lambda v: 0 <= v <= 2**30,
        'go_memstats_heap_objects': lambda v: 0 <= v <= 1e6,
        'go_memstats_heap_released_bytes': lambda v: 0 <= v <= 2**34,
        'go_memstats_heap_sys_bytes': lambda v: 0 <= v <= 2**34,
        'go_memstats_last_gc_time_seconds': lambda v: 1_000_000_000 <= v <= 4_000_000_000,
        'go_memstats_mallocs_total': lambda v: 0 <= v <= 1e9,
        'go_memstats_mcache_inuse_bytes': lambda v: 0 <= v <= 1_000_000,
        'go_memstats_mcache_sys_bytes': lambda v: 0 <= v <= 1_000_000,
        'go_memstats_mspan_inuse_bytes': lambda v: 0 <= v <= 1_000_000,
        'go_memstats_mspan_sys_bytes': lambda v: 0 <= v <= 10_000_000,
        'go_memstats_next_gc_bytes': lambda v: 0 < v <= 2**30,
        'go_memstats_other_sys_bytes': lambda v: 0 <= v <= 10_000_000,
        'go_memstats_stack_inuse_bytes': lambda v: 0 <= v <= 10_000_000,
        'go_memstats_stack_sys_bytes': lambda v: 0 <= v <= 10_000_000,
        'go_memstats_sys_bytes': lambda v: 0 < v <= 2**34,
        'percona_link_mongodb_copy_insert_batch_duration_seconds': lambda v: 0 <= v <= 1,
        'percona_link_mongodb_copy_insert_document_total': lambda v: 0 <= v <= 100_000,
        'percona_link_mongodb_copy_insert_size_bytes_total': lambda v: 0 <= v <= 2**30,
        'percona_link_mongodb_copy_read_batch_duration_seconds': lambda v: 0 <= v <= 1,
        'percona_link_mongodb_copy_read_document_total': lambda v: 0 <= v <= 100_000,
        'percona_link_mongodb_copy_read_size_bytes_total': lambda v: 0 <= v <= 2**30,
        'percona_link_mongodb_estimated_total_size_bytes': lambda v: 0 <= v <= 2**30,
        'percona_link_mongodb_events_processed_total': lambda v: 0 <= v <= 50_000,
        'percona_link_mongodb_initial_sync_lag_time_seconds': lambda v: v >= 0 and v <= 600,
        'percona_link_mongodb_lag_time_seconds': lambda v: v >= 0 and v <= 600,
        'percona_link_mongodb_process_cpu_seconds_total': lambda v: 0 <= v <= 600,
        'percona_link_mongodb_process_max_fds': lambda v: 0 < v <= 2**31,
        'percona_link_mongodb_process_network_receive_bytes_total': lambda v: 0 <= v <= 2**30,
        'percona_link_mongodb_process_network_transmit_bytes_total': lambda v: 0 <= v <= 2**30,
        'percona_link_mongodb_process_open_fds': lambda v: 1 <= v <= 100,
        'percona_link_mongodb_process_resident_memory_bytes': lambda v: 0 < v <= 2*2**30,
        'percona_link_mongodb_process_start_time_seconds': lambda v: 1_700_000_000 <= v <= 2_000_000_000,
        'percona_link_mongodb_process_virtual_memory_bytes': lambda v: 0 < v <= 10*2**30,
        'percona_link_mongodb_process_virtual_memory_max_bytes': lambda v: 0 < v <= 2**64,
    }
    missing = [key for key in expected_metrics_with_checks if key not in metrics]
    assert not missing, f"Missing expected metrics: {missing}"
    invalid = []
    for key, check in expected_metrics_with_checks.items():
        value = metrics[key]
        try:
            if not check(value):
                invalid.append((key, value))
        except Exception:
            invalid.append((key, value))
    assert not invalid, f"Invalid metric values: {invalid}"

@pytest.mark.usefixtures("start_cluster")
@pytest.mark.timeout(600,func_only=True)
def test_rs_plink_PML_T44(reset_state, srcRS, dstRS, plink):
    """
    Test to validate metrics returned by plink service
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = plink.start()
        assert result is True, "Failed to start plink service"
        metrics = plink.metrics()
        assert metrics["success"], f"Failed to fetch metrics after start: {metrics.get('error')}"
        assert_metrics(metrics["data"])
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = plink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        metrics = plink.metrics()
        assert metrics["success"], f"Failed to fetch metrics after start: {metrics.get('error')}"
        assert_metrics(metrics["data"])
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        time.sleep(5)
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
    result = plink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    metrics = plink.metrics()
    assert metrics["success"], f"Failed to fetch metrics after start: {metrics.get('error')}"
    assert_metrics(metrics["data"])
    result = plink.finalize()
    assert result is True, "Failed to finalize plink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    plink_error, error_logs = plink.check_plink_errors()
    assert plink_error is True, f"Plimk reported errors in logs: {error_logs}"