import os


def pytest_addoption(parser):
    parser.addoption(
        "--client-log",
        action="store_true",
        help="print each client URI and server response",
    )


def pytest_configure(config):
    enabled = config.getoption("--client-log") or os.environ.get("CLIENT_LOG", "").lower() in ("1", "true", "yes")
    if not enabled:
        return
    os.environ["CLIENT_LOG"] = "1"
    config.option.log_cli = True
    config.option.log_cli_level = "INFO"
    config.option.log_cli_format = "%(message)s"
