import pytest
import pymongo
import time
import docker

from cluster import Cluster
from data_generator import generate_dummy_data, create_all_types_db, stop_all_crud_operations, create_unique_index_collections

def _wait_for_clone_in_progress(csync, dst_connection, db_name, timeout=120):
    """
    Poll until the clone stage is confirmed actively in progress
    """
    dst_client = pymongo.MongoClient(dst_connection)
    start = time.time()
    while time.time() - start < timeout:
        status_response = csync.status()
        if status_response.get("success"):
            data = status_response.get("data", {})
            initial_sync = data.get("initialSync", {})
            if data.get("ok") and initial_sync.get("completed") is False:
                try:
                    collection_names = dst_client[db_name].list_collection_names()
                    if collection_names:
                        count = dst_client[db_name][collection_names[0]].estimated_document_count()
                        if count > 0:
                            Cluster.log(f"Clone in progress confirmed: {count} docs on dst in '{collection_names[0]}'")
                            return True
                except Exception:
                    pass
        time.sleep(0.5)
    Cluster.log(f"Timeout: clone stage did not reach in-progress state within {timeout}s")
    return False

def _wait_for_container_exit(csync, timeout=30):
    """
    Poll the csync container to confirm pcsm shutdown gracefully after SIGINT
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            container = csync.container
            container.reload()
            if container.status in ("exited", "dead"):
                exit_code = container.attrs["State"]["ExitCode"]
                assert exit_code == 0, f"csync exited with non-zero exit code {exit_code} — shutdown may not have been clean"
                return True
        except docker.errors.NotFound:
            return True
        time.sleep(0.5)
    Cluster.log(f"Timeout: csync container did not exit within {timeout}s")
    return False

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_clone_SIGINT_shutdown_PML_T79(start_cluster, src_cluster, dst_cluster, csync):
    """Verify SIGINT during clone causes a clean shutdown with no unexpected errors and no collection cancelled more than once."""
    generate_dummy_data(src_cluster.connection, db_name="test", num_collections=10, doc_size=200000, is_sharded=src_cluster.is_sharded)

    assert csync.start(), "Failed to start csync"
    assert _wait_for_clone_in_progress(csync, dst_cluster.connection, "test", timeout=120), \
        "Clone phase did not become active within timeout"

    Cluster.log("Sending SIGINT to csync container during cloning")
    csync.container.kill(signal="SIGINT")

    assert _wait_for_container_exit(csync, timeout=30), "csync container did not exit cleanly after SIGINT"

    csync_error, error_logs = csync.check_csync_errors()
    if not csync_error:
        cursor_errors = [line for line in error_logs if "Close change stream cursor" in line]
        if cursor_errors:
            pytest.fail("Cursor errors found in logs after SIGINT during clone:\n" + "\n".join(cursor_errors))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_replication_SIGIN_shutdown_PML_T80(start_cluster, src_cluster, dst_cluster, csync):
    """Verify SIGINT during replication causes a clean shutdown with no context errors and no cursor cleanup errors in logs."""
    operation_threads = []
    try:
        generate_dummy_data(src_cluster.connection, db_name="dummy", num_collections=5, doc_size=100000, is_sharded=src_cluster.is_sharded)
        _, crud_threads = create_all_types_db(src_cluster.connection, db_name="crud_db", start_crud=True, is_sharded=src_cluster.is_sharded)
        operation_threads.extend(crud_threads)

        assert csync.start(), "Failed to start csync"
        assert csync.wait_for_repl_stage(timeout=300), "Failed to reach replication stage"

        Cluster.log("Sending SIGINT to csync container during replication")
        csync.container.kill(signal="SIGINT")
        assert _wait_for_container_exit(csync, timeout=30), "csync container did not exit cleanly after SIGINT"
    finally:
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()

    csync_error, error_logs = csync.check_csync_errors()
    if not csync_error:
        cursor_errors = [line for line in error_logs if "Close change stream cursor" in line]
        if cursor_errors:
            pytest.fail("Cursor errors found in logs after SIGINT during replication:\n" + "\n".join(cursor_errors))

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_csync_finalize_SIGINT_shutdown_PML_T81(start_cluster, src_cluster, dst_cluster, csync):
    """Verify SIGINT during finalization causes a clean shutdown; context canceled errors indicate a bug and must not appear."""
    create_unique_index_collections(src_cluster.connection, db_name="finalize_db", num_collections=5, num_docs=100000, is_sharded=src_cluster.is_sharded)

    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(timeout=300), "Failed to reach replication stage"
    assert csync.wait_for_zero_lag(timeout=120), "Failed to reach zero replication lag before finalize"

    # Using http request due to finalize function locking until completion.
    exec_result = csync.container.exec_run("curl -s -X POST http://localhost:2242/finalize -d '{}'", demux=True)
    stdout, _ = exec_result.output
    response = stdout.decode("utf-8", errors="replace").strip() if stdout else ""
    Cluster.log(f"Finalize HTTP response: {response}")

    Cluster.log("Sending SIGINT to csync container during finalization")
    csync.container.kill(signal="SIGINT")

    assert _wait_for_container_exit(csync, timeout=30), "csync container did not exit cleanly after SIGINT"

    csync_error, error_logs = csync.check_csync_errors()
    if not csync_error:
        cursor_errors = [line for line in error_logs if "Close change stream cursor" in line]
        if cursor_errors:
            pytest.fail("Cursor errors found in logs after SIGINT during finalization:\n" + "\n".join(cursor_errors))
