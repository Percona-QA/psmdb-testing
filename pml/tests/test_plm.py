import os
from typing import re
import pytest
import json

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')


@pytest.fixture()
def plm_start(host):
    """Start and stop pbm-agent service

    :param host:
    :return:
    """

    result = host.run("percona-mongolink start")
    assert result.rc == 0, '"ok": true' in result.stdout
    result = host.run("journalctl -x")
    assert "Change Replication started" in result.stdout
    return True

@pytest.fixture()
def pml_status(host):
    """Start and stop pbm-agent service

    :param host:
    :return:
    """
    result = host.run("percona-mongolink status")
    assert result.rc == 0, result.stdout
    return result

@pytest.fixture()
def plm_finalize(host):
    """Start and stop pbm-agent service

    :param host:
    :return:
    """

    with host.sudo("root"):
        result = host.run("percona-mongolink finalize")
        assert result.rc == 0, '"ok": true' in result.stdout
        return result

def test_plm_binary(host):
    """Check pbm binary
    """
    file = host.file("/tmp/percona-mongolink/bin/percona-mongolink")
    assert file.user == "root"
    assert file.group == "root"
    try:
        assert file.mode == 0o775
    except AssertionError:
        pytest.xfail("Possible xfail")

def test_plm_start(plm_start):
    assert plm_start

def test_plm_status(pml_status):
    pml_status_output = json.loads(pml_status.stdout)
    assert "ok" in pml_status_output
    assert "state" in pml_status_output
    assert "info" in pml_status_output
    assert "lagTime" in pml_status_output
    assert "eventsProcessed" in pml_status_output
    assert "lastReplicatedOpTime" in pml_status_output
    assert "initialSync" in pml_status_output

    assert pml_status_output["ok"] is True
    assert pml_status_output["state"] == "running"
    assert pml_status_output["info"] == "Replicating Changes"
    assert pml_status_output["lagTime"] >= 0
    assert isinstance(pml_status_output["eventsProcessed"], int)

    sync = pml_status_output["initialSync"]
    assert sync["completed"] is True
    assert sync["cloneCompleted"] is True
    assert sync["estimatedCloneSize"] >= 0
    assert sync["clonedSize"] == sync["estimatedCloneSize"]


# TODO add correct start/stop test
def test_finalize_pml(plm_finalize):
    """Start and stop pbm agent

    :param plm_finalize:
    """
    assert json.loads(pml_status)["state"] == "finalized"

def test_pml_version(host):
    """Check that pbm version is not empty strings

    :param host:
    :return:
    """
    result = host.run("percona-mongolink version")
    assert result.rc == 0, result.stdout

    pattern = r"^v\d+\.\d+ [a-f0-9]{7} \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

    assert re.match(pattern, result), f"Output does not match expected format: {result}"


def test_pml_help(host):
    """Check that pbm have help message

    :param host:
    :return:
    """
    result = host.run("percona-mongolink help")
    assert result.rc == 0, result.stdout
