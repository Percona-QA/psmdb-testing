import json
import os
import time

import pytest
import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

MONGOT_VERSION = os.getenv('MONGOT_VERSION', '1.70.1').split('-')[0]
PACKAGE_NAME = 'percona-search-mongodb'
SERVICE_NAME = 'mongot'
READINESS_URL = 'http://localhost:8080/ready'
READINESS_RETRIES = 30
READINESS_DELAY = 5


def verify_mongot_readiness(host):
    last_error = ''
    for attempt in range(READINESS_RETRIES):
        result = host.run(f'curl {READINESS_URL}')
        if result.rc == 0:
            try:
                if json.loads(result.stdout) == {'status': 'SERVING'}:
                    return
                last_error = result.stdout
            except json.JSONDecodeError:
                last_error = result.stdout
        else:
            last_error = result.stderr
        if attempt + 1 < READINESS_RETRIES:
            time.sleep(READINESS_DELAY)
    pytest.fail(
        f'mongot readiness check failed after {READINESS_RETRIES} retries: {last_error}')


@pytest.fixture()
def start_stop_mongot(host):
    with host.sudo():
        result = host.run(f'systemctl stop {SERVICE_NAME}')
        assert result.rc == 0, result.stdout
        result = host.run(f'systemctl start {SERVICE_NAME}')
        assert result.rc == 0, result.stdout
        status = host.run(f'systemctl status {SERVICE_NAME}')
        assert status.rc == 0, status.stdout
    verify_mongot_readiness(host)
    return status


@pytest.fixture()
def restart_mongot(host):
    with host.sudo():
        result = host.run(f'systemctl restart {SERVICE_NAME}')
        assert result.rc == 0, result.stdout
        status = host.run(f'systemctl status {SERVICE_NAME}')
        assert status.rc == 0, status.stdout
    verify_mongot_readiness(host)
    return status


def test_mongot_package(host):
    with host.sudo():
        package = host.package(PACKAGE_NAME)
        assert package.is_installed
        assert MONGOT_VERSION in package.version, package.version


def test_mongot_version(host):
    with host.sudo():
        result = host.run('mongot --version')
        assert result.rc == 0, result.stderr
        assert MONGOT_VERSION in result.stdout, result.stdout


def test_mongot_service(host):
    service = host.service(SERVICE_NAME)
    assert service.is_running
    assert service.is_enabled


def test_mongot_start_stop_service(host, start_stop_mongot):
    assert start_stop_mongot.rc == 0, start_stop_mongot.stdout


def test_mongot_restart_service(host, restart_mongot):
    assert restart_mongot.rc == 0, restart_mongot.stdout
