import re
import pytest

from cluster import Cluster
from data_generator import generate_dummy_data
from data_integrity_check import compare_data

def _parse_segment_count(csync, namespace):
    raw_logs = csync.container.logs().decode("utf-8", errors="replace")
    pat = re.compile(
        rf'Segmenter for {re.escape(namespace)}:.*?segments:\s*~(\d+)')
    m = pat.search(raw_logs)
    if m is None:
        return None
    return int(m.group(1))

@pytest.mark.parametrize("cluster_configs", ["replicaset"], indirect=True)
@pytest.mark.timeout(900, func_only=True)
@pytest.mark.parametrize("scenario", [
    pytest.param({"cloneNumReadWorkers": 4}, id="auto"),
    pytest.param({"cloneSegmentSize": "500MB"}, id="explicit_500MB")])
def test_csync_PML_T99(start_cluster, src_cluster, dst_cluster, csync, scenario):
    """
    Asserts that a clone of collection above MinCloneSegmentSizeBytes
    (~480 MB) actually produces more than one segment
    """
    namespace_db = "dummy"
    namespace_coll = "collection_0"
    namespace = f"{namespace_db}.{namespace_coll}"
    generate_dummy_data(src_cluster.connection, namespace_db,
                        num_collections=1, doc_size=600000, batch_size=10000)
    assert csync.start(raw_args=scenario), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to start replication"
    segments = _parse_segment_count(csync, namespace)
    Cluster.log(f"Segmenter chose {segments} segments for {namespace} (raw_args={scenario!r})")
    assert segments is not None, f"No 'Segmenter for {namespace}: ... segments: ~N' line in PCSM logs"
    assert segments > 1, f"Segmenter produced only {segments} segment(s) for {namespace} with raw_args={scenario!r}, expected > 1"
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication after commit"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(src_cluster, dst_cluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"
