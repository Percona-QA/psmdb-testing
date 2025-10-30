import pytest

pytest_plugins = [
    "metrics_collector"
]

def pytest_addoption(parser):
    parser.addoption("--jenkins", action="store_true", default=False, help="Run tests marked as jenkins")

def pytest_collection_modifyitems(config, items):
    if config.getoption("--jenkins"):
        return
    skip_jenkins = pytest.mark.skip(reason="Skipped because --jenkins flag not set")
    for item in items:
        if "jenkins" in item.keywords:
            item.add_marker(skip_jenkins)
