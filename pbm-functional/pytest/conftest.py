import pytest

def pytest_addoption(parser):
    parser.addoption("--jenkins", action="store_true", default=False, help="Run tests marked as jenkins")

def get_cluster_config(setup_type):
    if setup_type == "replicaset":
        return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}
    elif setup_type == "sharded":
        return {
            "mongos": "mongos",
            "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
            "shards": [{"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}],
        }
    else:
        raise ValueError(f"Unknown setup type: {setup_type}")

@pytest.fixture(scope="function")
def cluster_configs(request):
    """
    Fixture that provides cluster configuration, must be parametrized using
    @pytest.mark.parametrize("cluster_configs", [...], indirect=True)
    """
    if not hasattr(request, "param"):
        raise ValueError(f"Test {request.node.name} uses cluster_configs but it's not parametrized")
    return get_cluster_config(request.param)

def pytest_collection_modifyitems(config, items):
    if config.getoption("--jenkins"):
        return
    skip_jenkins = pytest.mark.skip(reason="Skipped because --jenkins flag not set")
    for item in items:
        if "jenkins" in item.keywords:
            item.add_marker(skip_jenkins)