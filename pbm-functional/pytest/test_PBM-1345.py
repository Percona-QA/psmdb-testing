import docker
import pytest
import pymongo
import os
import time

from datetime import datetime, timezone
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
        "shards": [
            {
                "_id": "rs1",
                "members": [
                    {"host": "rs101", "mongod_extra_args": "--oplogSize 10"},
                ],
            },
            {"_id": "rs2", "members": [{"host": "rs201"}]},
        ],
    }

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy(cleanup_backups=True)
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        cluster.create()
        cluster.setup_pbm("/etc/pbm-fs.conf")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def _capture_oplog_top_ts(host):
    uri = f"mongodb://pbm:pbmpass@{host}:27017/?directConnection=true"
    client = pymongo.MongoClient(uri)
    top = client["local"]["oplog.rs"].find_one(sort=[("ts", -1)])
    return top["ts"] if top else None

def _read_oplog_oldest_ts(oplog, retries=5, backoff_s=0.2):
    last_exc = None
    for attempt in range(retries):
        try:
            doc = oplog.find_one(sort=[("ts", 1)])
            return doc["ts"] if doc else None
        except pymongo.errors.OperationFailure as exc:
            if exc.code != 136:
                raise
            last_exc = exc
            time.sleep(backoff_s * (attempt + 1))
    raise last_exc

def _force_pitr_oplog_gap(host, top_before_gap, deadline_s=300):
    assert top_before_gap is not None, "no oplog top captured before inserts"
    batch_size = 2000
    doc_pad = 4096
    uri = f"mongodb://pbm:pbmpass@{host}:27017/?directConnection=true"
    client = pymongo.MongoClient(uri)
    oplog = client["local"]["oplog.rs"]
    coll = client["test"]["pitr_test"]
    total = 0
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        oldest_ts = _read_oplog_oldest_ts(oplog)
        if oldest_ts and oldest_ts > top_before_gap:
            Cluster.log(
                f"Oplog gap confirmed on {host}: oldest ts {oldest_ts} > "
                f"{top_before_gap} after {total} inserted docs")
            return
        coll.insert_many(
            [{"p": "x" * doc_pad} for _ in range(batch_size)], ordered=False)
        total += batch_size
    raise AssertionError(
        f"Oplog on {host} did not wrap past {top_before_gap} after "
        f"{total} inserted docs within {deadline_s}s")

def _tail_agent_logs(container_names, tail=30000, since=None):
    dc = docker.from_env()
    kwargs = {"tail": tail}
    if since is not None:
        kwargs["since"] = since
    out = {}
    for name in container_names:
        try:
            out[name] = dc.containers.get(name).logs(**kwargs).decode("utf-8", errors="replace")
        except Exception as e:
            out[name] = f"<error fetching logs: {e}>"
    return out

@pytest.mark.timeout(600, func_only=True)
def test_pitr_PBM_T334(start_cluster, cluster):
    broken_rs = "rs1"
    broken_host = "rs101"
    healthy_agents = ("rs201", "rscfg01")
    skip_upload_log = "stopping due to cluster error, skipping final upload"
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    pitr_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(pitr_ts)
    top_before_gap = _capture_oplog_top_ts(broken_host)
    Cluster.log(
        f"Captured oplog top on {broken_host} before gap: {top_before_gap}")
    Cluster.log(f"Generating test docs on {broken_host} to create oplog gap")
    _force_pitr_oplog_gap(broken_host, top_before_gap)
    since_ts = datetime.now(timezone.utc)
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=1")
    log_hit = False
    deadline = time.time() + 300
    while time.time() < deadline:
        status = cluster.get_status()
        pitr_error = ((status.get("pitr") or {}).get("error") or "").strip()
        logs = _tail_agent_logs(healthy_agents, since=since_ts)
        if any(skip_upload_log in v for v in logs.values()):
            log_hit = True
            Cluster.log("Found skip-final-upload log on a healthy PITR agent")
            break
        if pitr_error:
            Cluster.log("pbm pitr.error: " + pitr_error)
        time.sleep(3)
    assert log_hit, (
        f"expected '{skip_upload_log}' on at least one healthy PITR agent "
        f"after {broken_rs} oplog was truncated past lastTS")
    pbm_pitr_dir = "/backups/pbmPitr"
    since_epoch = since_ts.timestamp()
    new_pitr_files = []
    if os.path.isdir(pbm_pitr_dir):
        for root, _dirs, files in os.walk(pbm_pitr_dir):
            for fname in files:
                fpath = os.path.join(root, fname)
                try:
                    if os.path.getmtime(fpath) >= since_epoch:
                        new_pitr_files.append(fpath)
                except OSError:
                    pass
    assert not new_pitr_files, (
        f"Regression: new oplog chunks appeared under {pbm_pitr_dir} "
        f"after {broken_rs} oplog gap was forced; expected no fresh uploads. "
        f"Files: {new_pitr_files}")
    Cluster.log("Finished successfully")