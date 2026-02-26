import pytest
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture
def use_metadata_host(request):
    return request.param

@pytest.fixture(scope="function")
def start_cluster(cluster, request, use_metadata_host):
    try:
        cluster.destroy()
        cluster.extra_environment = ({"GCE_METADATA_HOST": "fake-metadata:8080"} if use_metadata_host else {})
        cluster.create()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.parametrize(
    "use_metadata_host,expected_in_error",
    [(False, "could not find default credentials"), (True, "401: Invalid Credentials")],
    ids=["no_metadata", "invalid_metadata"],
    indirect=["use_metadata_host"])
@pytest.mark.timeout(100, func_only=True)
def test_PBM_T314(start_cluster, cluster, use_metadata_host, expected_in_error):
    result = cluster.exec_pbm_cli("config --file=/etc/pbm-gcs-work-iden.conf --wait")
    combined = (result.stderr or "") + (result.stdout or "")
    assert result.rc != 0, f"Expected config to fail, got rc=0: {combined}"
    assert expected_in_error.lower() in combined.lower(), (
        f"Expected {expected_in_error!r} in output. Got: {combined}")