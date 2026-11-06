import threading
import time

import pytest
from data_generator import create_all_types_db, stop_all_crud_operations
from data_integrity_check import compare_data

ROLE_TRANSITIONS = "percona_clustersync_mongodb_ha_role_transitions_total"
TERM = "percona_clustersync_mongodb_ha_term"
OUTAGE_SECS = 20

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.parametrize("csync_env", [
    pytest.param({}, id="ttl_shorter_than_outage"),
    pytest.param({"PCSM_HA_LEASE_TTL": "60s"}, id="ttl_longer_than_outage")], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_target_outage_lease_ttl_PML_T144(start_cluster, src_cluster, dst_cluster, csync, csync_env):
    """
    PCSM-405: check that configured HA lease TTL decides whether PCSM survives target outage
    TTL shorter than outage (10s): PCSM steps down to STANDBY, then becomes ACTIVE once target is back
    TTL longer than outage (60s): PCSM stays ACTIVE, same lease term, no role change
    """
    lease_lost = "PCSM_HA_LEASE_TTL" not in csync_env
    operation_threads = []
    try:
        _, threads = create_all_types_db(src_cluster.connection, "init_test_db", start_crud=True,
                                         is_sharded=src_cluster.is_sharded)
        operation_threads += threads
        assert csync.wait_for_role("ACTIVE"), "csync server didn't come up"
        assert csync.start(), "failed to start csync"
        assert csync.wait_for_repl_stage(), "failed to reach the replication stage"
        metrics = csync.metrics()["data"]
        transitions_before = metrics[ROLE_TRANSITIONS]
        term_before = metrics[TERM]
        outage = threading.Thread(target=dst_cluster.network_interruption, kwargs={"delay": OUTAGE_SECS})
        outage.start()
        stepped_down = False
        try:
            deadline = time.time() + OUTAGE_SECS
            while time.time() < deadline and not (lease_lost and stepped_down):
                stepped_down = stepped_down or csync.role() == "STANDBY"
                time.sleep(1)
        finally:
            outage.join()
        if lease_lost:
            assert stepped_down, (
                f"csync kept lease through a {OUTAGE_SECS}s target outage with default TTL: "
                f"{csync.logs(tail=30)}")
        else:
            assert not stepped_down, (
                f"csync lost lease in a {OUTAGE_SECS}s target outage despite raised TTL: "
                f"{csync.logs(tail=30)}")
        assert csync.wait_for_role("ACTIVE", timeout=120), "csync is not ACTIVE once the target is back"
        metrics = csync.metrics()["data"]
        if lease_lost:
            assert metrics[ROLE_TRANSITIONS] > transitions_before, (
                "the lease was lost but no role transition was counted")
        else:
            assert metrics[ROLE_TRANSITIONS] == transitions_before, "csync changed role during the outage"
            assert metrics[TERM] == term_before, "lease term changed during the outage"
        _, threads = create_all_types_db(src_cluster.connection, "repl_test_db", start_crud=True,
                                         is_sharded=src_cluster.is_sharded)
        operation_threads += threads
    finally:
        stop_all_crud_operations()
        for thread in operation_threads:
            thread.join()
    assert csync.wait_for_zero_lag(), "failed to catch up on replication"
    assert csync.finalize(), "failed to finalize"
    result, mismatch = compare_data(src_cluster, dst_cluster)
    assert result is True, f"data mismatch after the target outage: {mismatch}"
