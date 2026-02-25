import pytest
import pymongo
import time
import os
import re

from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203", "arbiterOnly": True}]},
        ],
    }

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm()
        client = pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True

    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600, func_only=True)
def test_pitr_PBM_T313(start_cluster, cluster):
    client = pymongo.MongoClient(cluster.connection)
    for i in range(5):
        client["test"]["test"].insert_one({"x": i})
    assert client["test"]["test"].count_documents({}) == 5
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    time.sleep(5)
    profile = cluster.exec_pbm_cli("profile add filesystem /etc/pbm-fs-profile.conf --wait")
    assert profile.rc == 0, profile.stderr
    assert "OK" in profile.stdout, profile.stdout
    profile = cluster.exec_pbm_cli("profile sync filesystem")
    assert profile.rc == 0, profile.stderr
    assert "OK" in profile.stdout, profile.stdout
    Cluster.log("Filesystem profile added and synced")
    time.sleep(2)
    fs_backup = cluster.make_backup("logical --profile filesystem")
    Cluster.log("Check if the backup actually was created on the filesystem storage")
    assert os.path.isdir("/backups/" + fs_backup)
    assert os.path.isfile("/backups/" + fs_backup + ".pbm.json")
    # PITR should not pause for profile backup
    logs_result = cluster.exec_pbm_cli("logs -sD -t0")
    logs = (logs_result.stdout or "") + (logs_result.stderr or "")
    assert "pausing/stopping with last_ts" not in logs, (
        "PITR should not pause for profile backup")
    # Secondary node should be deprioritized for backup if it's running
    # PITR unless it's the only available secondary node due to PSA setup
    backup_ctx = "backup/" + fs_backup
    rs1_line = rs2_line = None
    for line in logs.split("\n"):
        if backup_ctx not in line:
            continue
        if "nomination list for rs1:" in line:
            rs1_line = line
            Cluster.log("rs1 line: " + line)
        if "nomination list for rs2:" in line:
            rs2_line = line
            Cluster.log("line: " + line)
    def _nomination_list(line, rs):
        m = re.search(r"nomination list for %s:\s*(\[.*\])" % rs, line)
        return m.group(1).strip() if m else None
    assert rs1_line, "No nomination list for rs1 in logs (backup %s)" % fs_backup
    rs1_list = _nomination_list(rs1_line, "rs1")
    assert rs1_list, "Could not parse rs1 list: %s" % rs1_line
    first_tier = rs1_list[2 : rs1_list.index("]", 1)]
    assert " " not in first_tier, "rs1 first tier must have one node only: %s" % rs1_list
    assert rs2_line, "No nomination list for rs2 in logs (backup %s)" % fs_backup
    rs2_list = _nomination_list(rs2_line, "rs2")
    assert rs2_list == "[[rs202:27017] [rs201:27017]]", "rs2 list: got %s" % rs2_list
