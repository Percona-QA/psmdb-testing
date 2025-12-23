import pytest
import os
import time
import glob
import subprocess
from datetime import datetime, timezone

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.setup_pbm('/etc/pbm-fs.conf')
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy()

@pytest.mark.parametrize('oplog_action', ['remove_access', 'truncate_file', 'remove_file'])
@pytest.mark.timeout(300, func_only=True)
def test_pitr_PBM_T306(start_cluster, cluster, oplog_action):
    """
    Test case PBM-1488: PITR continues saving oplog when copying oplog from backup fails
    Steps:
    1. Create logical backup, enable PITR and create second logical backup
    2. Remove read permissions or remove oplog files in 2nd backup to prevent copying and verify PITR logs
    3. Remove second backup and perform PITR restore to ensure PITR timeline isn't broken
    """
    cluster.make_backup('logical')
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(2)
    backup2 = cluster.make_backup('logical')
    oplog_folder = f"/backups/{backup2}/rs1/oplog"
    if os.path.exists(oplog_folder):
        if oplog_action == 'remove_access':
            oplog_files = glob.glob(f"{oplog_folder}/*")
            for file_path in oplog_files:
                subprocess.run(['chmod', '000', file_path], check=False)
            Cluster.log(f"Removed read access on files in oplog folder: {oplog_folder}")
        elif oplog_action == 'remove_file':
            oplog_files = glob.glob(f"{oplog_folder}/*")
            for file_path in oplog_files:
                if os.path.isfile(file_path) or os.path.isdir(file_path):
                    subprocess.run(['rm', '-f', file_path], check=False)
            Cluster.log(f"Removed oplog files from folder: {oplog_folder}")
        elif oplog_action == 'truncate_file':
            oplog_files = glob.glob(f"{oplog_folder}/*")
            for file_path in oplog_files:
                if os.path.isfile(file_path):
                    with open(file_path, 'w') as f:
                        f.truncate(0)
            Cluster.log(f"Truncated oplog files in folder: {oplog_folder}")
    start_ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    copy_oplog_seen = False
    catchup_seen = False
    start_ok_seen = False
    timeout = time.time() + 60
    while time.time() < timeout:
        logs = cluster.exec_pbm_cli("logs -sD -e pitr")
        log_lines = logs.stdout.splitlines()
        for line in log_lines:
            line_parts = line.split(' ', 1)
            if len(line_parts) < 2:
                continue
            log_ts_str = line_parts[0].rstrip('Z')
            if log_ts_str < start_ts_str:
                continue
            line_lower = line.lower()
            if "copy oplog from" in line_lower and not copy_oplog_seen:
                Cluster.log(f"Found message: {line}")
                copy_oplog_seen = True
            if "try to do pitr catch-up" in line_lower and not catchup_seen:
                Cluster.log(f"Found message: {line}")
                catchup_seen = True
            if "start_ok" in line_lower and not start_ok_seen:
                Cluster.log(f"Found message: {line}")
                start_ok_seen = True
        if start_ok_seen:
            break
        time.sleep(1)
    assert start_ok_seen, "PITR didn't continue after oplog copy failure"
    pitr = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    Cluster.log("Time for PITR is: " + pitr)
    cluster.disable_pitr(pitr)
    Cluster.log(f"Removing backup: {backup2}")
    cluster.exec_pbm_cli(f"delete-backup {backup2} --force --yes")
    pitr = " --time=" + pitr
    cluster.make_restore(pitr, check_pbm_status=True)
    Cluster.log('Finished successfully')