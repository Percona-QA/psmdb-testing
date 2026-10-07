import json
import os
import re
import time

import pymongo
import pytest
import testinfra
from cluster import Cluster

HISTORY_ERROR = ("can't find incremental backup history. Previous backup was made on another node. "
                 "You can make a new base incremental backup to start a new history.")
CONVERGE_TIMEOUT = "reached converge timeout"


@pytest.fixture(scope="package")
def config():
    return { "mongos": "mongos",
             "configserver":
                            {"_id": "rscfg", "members": [{"host":"rscfg01"}]},
             "shards":[
                            {"_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]},
                            {"_id": "rs2", "members": [{"host":"rs201"},{"host": "rs202"},{"host": "rs203" }]}
                      ]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm("/etc/pbm-fs.conf")
        client=pymongo.MongoClient(cluster.connection)
        client.admin.command("enableSharding", "test")
        client.admin.command("shardCollection", "test.test", key={"_id": "hashed"})
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def stop_agent_and_wait(cluster, host, wait=90):
    """Stop pbm-agent on host and wait until pbm status reports it as not ok"""
    n = testinfra.get_host("docker://" + host)
    n.check_output("supervisorctl stop pbm-agent")
    Cluster.log("Stopped pbm-agent on " + host)
    timeout = time.time() + wait
    while True:
        nodes = [node for rs in cluster.get_status()["cluster"] for node in rs["nodes"]]
        node = next(nd for nd in nodes if nd["host"].split(":")[0] == host)
        if not node["ok"]:
            Cluster.log(f"pbm status reports agent on {host} as not ok: {node}")
            return
        assert time.time() < timeout, f"pbm status still reports agent on {host} as ok: {node}"
        time.sleep(1)

def wait_backup_terminal(cluster, name, wait=120):
    timeout = time.time() + wait
    while True:
        for snapshot in cluster.get_status()["backups"]["snapshot"] or []:
            if snapshot["name"] == name and snapshot["status"] in ("done", "error", "canceled"):
                return snapshot
        assert time.time() < timeout, f"Backup {name} didn't reach a terminal status"
        time.sleep(1)

def check_backup_reports_history_error(cluster, name, failed_rs):
    """Check describe-backup and pbm logs show the real shard error, not the converge timeout,
    and describe-backup has an entry for the failed replset"""
    wait_backup_terminal(cluster, name)
    result = cluster.exec_pbm_cli(f"describe-backup {name} --out=json")
    assert result.rc == 0, result.stdout + result.stderr
    desc = json.loads(result.stdout)
    Cluster.log(f"describe-backup {name}: {desc}")
    assert desc["status"] == "error"
    assert HISTORY_ERROR in desc["error"], f"describe-backup doesn't show the shard error: {desc['error']}"
    assert CONVERGE_TIMEOUT not in desc["error"], f"describe-backup shows the converge timeout: {desc['error']}"
    rs_meta = next((rs for rs in desc.get("replsets") or [] if rs["name"] == failed_rs), None)
    assert rs_meta, f"describe-backup has no entry for the failed replset {failed_rs}: {desc.get('replsets')}"
    assert rs_meta["status"] == "error", f"{failed_rs} isn't marked as error: {rs_meta}"
    assert HISTORY_ERROR in rs_meta.get("error", ""), f"{failed_rs} entry doesn't show its error: {rs_meta}"

    result = cluster.exec_pbm_cli(f"logs -sD -t0 -e backup/{name}")
    assert result.rc == 0, result.stdout + result.stderr
    assert HISTORY_ERROR in result.stdout, f"pbm logs don't show the shard error:\n{result.stdout}"
    assert CONVERGE_TIMEOUT not in result.stdout, f"pbm logs show the converge timeout:\n{result.stdout}"

def backup_node(cluster, name, rs):
    result = cluster.exec_pbm_cli(f"describe-backup {name} --out=json")
    assert result.rc == 0, result.stdout + result.stderr
    return next(r["node"] for r in json.loads(result.stdout)["replsets"] if r["name"] == rs).split(":")[0]

@pytest.mark.timeout(3900,func_only=True)
def test_agent_down_on_base_node_PBM_T384(start_cluster,cluster):
    """Verify an incremental backup fails fast with the real shard error when the base-backup node's agent is down"""

    client = pymongo.MongoClient(cluster.connection)
    collection = client["test"]["test"]
    collection.insert_many([{"data": i} for i in range(1000)])
    base_backup = cluster.make_backup("incremental --base")
    base_node = backup_node(cluster, base_backup, "rs1")
    Cluster.log(f"Base backup for rs1 was taken on {base_node}")
    stop_agent_and_wait(cluster, base_node)

    start = time.time()
    result = cluster.exec_pbm_cli("backup --type=incremental --wait")
    duration = time.time() - start
    output = result.stdout + result.stderr
    assert result.rc != 0, f"Incremental backup unexpectedly succeeded:\n{output}"
    assert CONVERGE_TIMEOUT not in output, f"--wait output shows the converge timeout:\n{output}"
    assert HISTORY_ERROR in output, f"--wait output doesn't show the shard error:\n{output}"
    assert duration < 30, f"Backup took {duration:.1f}s to fail, expected well under the 33s converge timeout"
    name = re.search(r'Starting backup "([^"]+)"', output).group(1)
    check_backup_reports_history_error(cluster, name, "rs1")

    # New base backup starts a fresh chain on a working node, and the next incremental uses it
    new_base = cluster.make_backup("incremental --base")
    new_base_node = backup_node(cluster, new_base, "rs1")
    assert new_base_node != base_node, f"New base backup used {new_base_node}, whose agent is down"
    collection.insert_many([{"data": i} for i in range(2000, 3000)])
    new_incr = cluster.make_backup("incremental")
    new_incr_node = backup_node(cluster, new_incr, "rs1")
    assert new_incr_node == new_base_node, f"Incremental used {new_incr_node}, but the new base is on {new_base_node}"
    Cluster.log(f"New chain: base {new_base} and incremental {new_incr} on {new_incr_node}")
    Cluster.log("Finished successfully")
