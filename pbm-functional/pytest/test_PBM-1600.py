from datetime import datetime

import pexpect
import pymongo
import pytest

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="module")
def start_cluster(cluster, request):
    try:
        cluster.destroy(cleanup_backups=True)
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.fixture(scope="module")
def restore_backup(start_cluster, cluster):
    cluster.wait_pbm_status()
    return cluster.make_backup("logical")

@pytest.fixture(scope="module")
def pitr_restore_time(start_cluster, cluster):
    cluster.wait_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["col"].insert_many([{"i": i} for i in range(100)])
    client.close()
    pitr_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(time_param=pitr_time)
    return pitr_time

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("flag,prompt_response,expect_deleted", [
    ("-y", None, True),
    ("--yes", None, True),
    ("", "y", True),
    ("", "n", False),
], ids=["with -y", "with --yes", "no flag accept", "no flag deny"])
def test_delete_backup_confirmation_PBM_T325(start_cluster, cluster, flag, prompt_response, expect_deleted):
    """
    Verify confirmation prompt behaviour for pbm delete-backup command
    """
    backup = cluster.make_backup("logical")

    if flag:
        result = cluster.exec_pbm_cli(f"delete-backup {flag} {backup}")
        output = result.stdout + result.stderr
        assert result.rc == 0, output
        assert "canceled" not in output.lower()
    else:
        child = pexpect.spawn(f"docker exec -it {cluster.pbm_cli} pbm delete-backup {backup}")
        child.expect(r"\[y/N\]")
        child.sendline(prompt_response)
        child.expect(pexpect.EOF)
        child.close()
        output = child.before.decode()
        assert child.exitstatus == 0, output
        if not expect_deleted:
            assert "canceled" in output.lower()

    status = cluster.exec_pbm_cli("status -s backups")
    if expect_deleted:
        assert backup not in status.stdout
    else:
        assert backup in status.stdout

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("flag,prompt_response,expect_deleted", [
    ("-y", None, True),
    ("--yes", None, True),
    ("", "y", True),
    ("", "n", False),
], ids=["with -y", "with --yes", "no flag accept", "no flag deny"])
def test_delete_pitr_confirmation_PBM_T326(start_cluster, cluster, flag, prompt_response, expect_deleted):
    """
    Verify confirmation prompt behaviour for pbm delete-pitr command
    """
    cluster.wait_pbm_status()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["col"].insert_many([{"i": i} for i in range(100)])
    client.close()
    pitr_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(time_param=pitr_time)

    if flag:
        result = cluster.exec_pbm_cli(f"delete-pitr --all {flag} --wait")
        output = result.stdout + result.stderr
        assert result.rc == 0, output
        assert "canceled" not in output.lower()
    else:
        child = pexpect.spawn(f"docker exec -it {cluster.pbm_cli} pbm delete-pitr --all --wait")
        child.expect(r"\[y/N\]")
        child.sendline(prompt_response)
        child.expect(pexpect.EOF)
        child.close()
        output = child.before.decode()
        assert child.exitstatus == 0, output
        if not expect_deleted:
            assert "canceled" in output.lower()

    status = cluster.exec_pbm_cli("status -s backups")
    if expect_deleted:
        assert "pitr chunks" not in status.stdout.lower()
    else:
        assert "pitr chunks" in status.stdout.lower()

@pytest.mark.timeout(600, func_only=True)
@pytest.mark.parametrize("flag,prompt_response,expect_restored", [
    ("-y", None, True),
    ("--yes", None, True),
    ("", "y", True),
    ("", "n", False),
], ids=["with -y", "with --yes", "no flag accept", "no flag deny"])
def test_restore_confirmation_PBM_T327(restore_backup, cluster, flag, prompt_response, expect_restored):
    """
    Verify confirmation prompt behaviour for pbm restore command
    """
    cluster.wait_pbm_status()

    if flag:
        result = cluster.exec_pbm_cli(f"restore {flag} {restore_backup} --wait")
        output = result.stdout + result.stderr
        assert result.rc == 0, output
        assert "canceled" not in output.lower()
        assert "starting restore" in output.lower()
    else:
        child = pexpect.spawn(f"docker exec -it {cluster.pbm_cli} pbm restore {restore_backup} --wait")
        child.expect(r"\[y/N\]")
        child.sendline(prompt_response)
        child.expect(pexpect.EOF, timeout=300)
        child.close()
        output = child.before.decode()
        assert child.exitstatus == 0, output
        if expect_restored:
            assert "starting restore" in output.lower()
        else:
            assert "canceled" in output.lower()

@pytest.mark.timeout(600, func_only=True)
@pytest.mark.parametrize("flag,prompt_response,expect_restored", [
    ("-y", None, True),
    ("--yes", None, True),
    ("", "y", True),
    ("", "n", False),
], ids=["with -y", "with --yes", "no flag accept", "no flag deny"])
def test_pitr_restore_confirmation_PBM_T328(pitr_restore_time, cluster, flag, prompt_response, expect_restored):
    """
    Verify restore confirmation prompt behaviour for pbm restore --time
    """
    cluster.wait_pbm_status()

    if flag:
        result = cluster.exec_pbm_cli(f"restore {flag} --time {pitr_restore_time} --wait")
        output = result.stdout + result.stderr
        assert result.rc == 0, output
        assert "canceled" not in output.lower()
        assert "starting restore" in output.lower()
    else:
        child = pexpect.spawn(f"docker exec -it {cluster.pbm_cli} pbm restore --time {pitr_restore_time} --wait")
        child.expect(r"\[y/N\]")
        child.sendline(prompt_response)
        child.expect(pexpect.EOF, timeout=300)
        child.close()
        output = child.before.decode()
        assert child.exitstatus == 0, output
        if expect_restored:
            assert "starting restore" in output.lower()
        else:
            assert "canceled" in output.lower()

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("flag,prompt_response,expect_deleted", [
    ("-y", None, True),
    ("--yes", None, True),
    ("", "y", True),
    ("", "n", False),
], ids=["with -y", "with --yes", "no flag accept", "no flag deny"])
def test_cleanup_confirmation_PBM_T329(start_cluster, cluster, flag, prompt_response, expect_deleted):
    """
    Verify confirmation prompt behaviour for pbm cleanup command
    """
    cluster.wait_pbm_status()
    cluster.make_backup("logical")
    older_than = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    if flag:
        result = cluster.exec_pbm_cli(f"cleanup --older-than {older_than} {flag} --wait")
        output = result.stdout + result.stderr
        assert result.rc == 0, output
        assert "canceled" not in output.lower()
    else:
        child = pexpect.spawn(f"docker exec -it {cluster.pbm_cli} pbm cleanup --older-than {older_than} --wait")
        child.expect(r"\[y/N\]")
        child.sendline(prompt_response)
        child.expect(pexpect.EOF)
        child.close()
        output = child.before.decode()
        assert child.exitstatus == 0, output
        if not expect_deleted:
            assert "canceled" in output.lower()

    status = cluster.exec_pbm_cli("status -s backups")
    if expect_deleted:
        assert "no snapshots or pitr chunks" in status.stdout.lower()
    else:
        assert "no snapshots or pitr chunks" not in status.stdout.lower()
