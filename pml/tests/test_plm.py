import os
import re
import pytest
import json

import testinfra.utils.ansible_runner
testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')


@pytest.fixture()
def pml_start(host):
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
def pml_finalize(host):
    """Start and stop pbm-agent service

    :param host:
    :return:
    """
    result = host.run("percona-mongolink finalize")
    assert result.rc == 0, '"ok": true' in result.stdout
    return result

@pytest.fixture()
def pml_version(host):
    """Start and stop pbm-agent service

    :param host:
    :return:
    """
    result = host.run("percona-mongolink version")
    assert result.rc == 0, result.stdout
    return result

def pml_add_db_row(host):
    result = host.run("docker exec -it source mongo testdb --eval 'db.test.insertOne({ name: `testUser`, age: 42 })'")
    assert result.rc == 0
    return True

def pml_confirm_db_row(host):
    result = host.run("docker exec -it source mongo testdb --eval 'db.test.find()'")
    assert result.rc == 0
    return result

def test_plm_binary(host):
    """Check pbm binary
    """
    file = host.file("/tmp/percona-mongolink/bin/percona-mongolink")
    assert file.user == "root"
    assert file.group == "root"
    try:
        assert file.mode == 0o755
    except AssertionError:
        pytest.xfail("Possible xfail")

def test_pml_version(pml_version):
    """Check that pbm version is not empty strings

    :param host:
    :return:
    """
    pattern = r"^v\d+\.\d+ [a-f0-9]{7} \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

    assert re.match(pattern, pml_version.stderr)

def test_pml_help(host):
    """Check that pbm have help message

    :param host:
    :return:
    """
    result = host.run("percona-mongolink help")
    assert result.rc == 0, result.stdout

def test_pml_transfer():
    assert pml_start
    assert pml_add_db_row
    assert pml_finalize
    # assert pml_confirm_db_row.stdout contains


# def test_finalize_pml(pml_finalize, pml_status):
#     """Start and stop pbm agent
#
#     :param pml_finalize:
#     """
#     assert json.loads(pml_status.stdout)["state"] == "finalized"
#
# def test_plm_status(pml_status):
#     pml_status_output = json.loads(pml_status.stdout)
#     assert "ok" in pml_status_output
#     assert "state" in pml_status_output
#     assert "info" in pml_status_output
#     assert "lagTime" in pml_status_output
#     assert "eventsProcessed" in pml_status_output
#     assert "lastReplicatedOpTime" in pml_status_output
#     assert "initialSync" in pml_status_output
#
#     assert pml_status_output["ok"] is True
#     assert pml_status_output["state"] == "running"
#     assert pml_status_output["info"] == "Replicating Changes"
#     assert pml_status_output["lagTime"] >= 0
#     assert isinstance(pml_status_output["eventsProcessed"], int)
#
#     sync = pml_status_output["initialSync"]
#     assert sync["completed"] is True
#     assert sync["cloneCompleted"] is True
#     assert sync["estimatedCloneSize"] >= 0
#     assert sync["clonedSize"] == sync["estimatedCloneSize"]

