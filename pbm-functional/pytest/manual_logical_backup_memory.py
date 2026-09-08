import json
import time

import pymongo
import pytest
import testinfra
from bson.binary import Binary
from cluster import Cluster

# Not collected by CI (filename is not test_*.py). Run locally:
#   pytest -s manual_logical_backup_memory.py
#
# STR: PBM 2.15 MinIO ConcurrentStreamParts uses
# NumThreads = runtime.NumCPU()/2, so RSS may scale with core count.
#
# Customer (PSMDB 8 + PBM 2.15, operator):
#   ~100GB dump  -> ~4GB  pbm-agent RSS
#   ~1TB dump    -> ~120GB
#   ~1.6TB dump  -> ~160GB
#
# strace intercepts sched_getaffinity (Go runtime.NumCPU) and reports
# a fake CPU count. Native MinIO storage is required (not type: s3).
CPU_COUNTS = (2, 8, 32, 64, 128)
DATA_BYTES = 10 * 1024 * 1024 * 1024
DOC_BYTES = 1024 * 1024
INSERT_BATCH = 32
MINIO_PART_SIZE = 130037383
BACKUP_WAIT_SEC = 7200

_AGENT_TAIL = '/usr/bin/pbm-agent --mongodb-uri "%(ENV_PBM_MONGODB_URI)s"'


def _host(name):
    return testinfra.get_host(f"docker://{name}")


def _pretty_bytes(n):
    n = float(n)
    for unit, step in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if abs(n) >= step or unit == "KiB":
            prec = 2 if unit == "GiB" else 1
            return f"{n / step:.{prec}f}{unit}"
    return f"{int(n)}B"


def _strace_fake_affinity(cpus):
    nbytes = max(8, (cpus + 7) // 8)
    poke = ((1 << cpus) - 1).to_bytes(nbytes, "little").hex()
    return (
        "strace -f -qq -e trace=sched_getaffinity "
        f"-e inject=sched_getaffinity:retval={nbytes}:poke_exit=@arg3={poke}"
    )


def _set_agent_fake_cpus(n, cpus):
    """Restart pbm-agent so runtime.NumCPU() sees `cpus` via sched_getaffinity."""
    prefix = _strace_fake_affinity(cpus)
    probed = n.check_output(f"{prefix} nproc").strip()
    Cluster.log(f"nproc under sched_getaffinity inject ({cpus} bits): {probed}")
    assert probed == str(cpus), (
        f"strace sched_getaffinity inject failed: nproc={probed}, want {cpus}"
    )

    cmd = f"{prefix} {_AGENT_TAIL}"
    n.check_output(
        f"sed -i 's|^command=.*|command={cmd}|' /etc/supervisord.d/pbm-agent.ini"
    )
    n.check_output("supervisorctl reread")
    n.check_output("supervisorctl update")
    Cluster.restart_pbm_agent("rs101")


def _pbm_agent_rss_kb(host_name):
    n = _host(host_name)
    pid = n.check_output("pgrep -nx pbm-agent").strip()
    rss = n.check_output(f"awk '/^VmRSS:/ {{print $2}}' /proc/{pid}/status").strip()
    return int(rss)


def _insert_single_collection(client, total_bytes):
    ndocs = total_bytes // DOC_BYTES
    payload = Binary(b"x" * DOC_BYTES)
    coll = client["memtest"]["data"]
    for start in range(0, ndocs, INSERT_BATCH):
        end = min(start + INSERT_BATCH, ndocs)
        coll.insert_many(
            [{"_id": i, "payload": payload} for i in range(start, end)],
            ordered=False,
        )
    Cluster.log(f"Inserted {ndocs} docs into memtest.data (~{_pretty_bytes(total_bytes)})")


def _wait_backup_running(cluster, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        running = cluster.get_status().get("running") or {}
        if running.get("type") == "backup":
            return
        time.sleep(1)
    assert False, "timed out waiting for backup to start running"


def _logical_backup_peak_rss_kb(cluster):
    idle = _pbm_agent_rss_kb("rs101")
    result = cluster.exec_pbm_cli("backup --type=logical --num-parallel-collections=1 --out=json")
    assert result.rc == 0, result.stdout + result.stderr
    name = json.loads(result.stdout)["name"]
    Cluster.log(f"Started logical backup {name}, idle RSS={_pretty_bytes(idle * 1024)}")
    _wait_backup_running(cluster)

    peak = idle
    samples = 0
    deadline = time.time() + BACKUP_WAIT_SEC
    while time.time() < deadline:
        rss = _pbm_agent_rss_kb("rs101")
        samples += 1
        peak = max(peak, rss)
        for snap in cluster.get_status().get("backups", {}).get("snapshot") or []:
            if snap.get("name") != name:
                continue
            if snap.get("status") == "done":
                Cluster.log(
                    f"Backup {name} done, peak RSS={_pretty_bytes(peak * 1024)} "
                    f"(idle={_pretty_bytes(idle * 1024)}, samples={samples})"
                )
                return idle, peak
            if snap.get("status") == "error":
                logs = _host(cluster.pbm_cli).check_output("pbm logs -t0")
                assert False, f"backup {name} failed: {snap.get('error')}\n{logs}"
        time.sleep(1)
    assert False, f"timed out sampling RSS for backup {name}"


def _format_rss_table(rows):
    header = ("cpus", "threads", "idle", "peak", "delta")
    body = [
        (
            str(r["cpus"]),
            str(r["threads"]),
            _pretty_bytes(r["idle"] * 1024),
            _pretty_bytes(r["peak"] * 1024),
            _pretty_bytes(r["delta"] * 1024),
        )
        for r in rows
    ]
    widths = [max(len(header[i]), max(len(row[i]) for row in body)) for i in range(5)]
    lines = [
        "  ".join(header[i].ljust(widths[i]) for i in range(5)),
        "  ".join("-" * widths[i] for i in range(5)),
    ]
    for row in body:
        lines.append("  ".join(row[i].ljust(widths[i]) for i in range(5)))
    return "\n".join(lines)


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
        cluster.setup_pbm("/etc/pbm-minio-provider.conf")
        result = cluster.exec_pbm_cli(
            f"config --set storage.minio.partSize={MINIO_PART_SIZE} --wait"
        )
        assert result.rc == 0, result.stdout + result.stderr
        Cluster.log(f"Set storage.minio.partSize={MINIO_PART_SIZE} ({_pretty_bytes(MINIO_PART_SIZE)})")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(18000, func_only=True)
def test_logical_backup_rss_by_cpu_count(start_cluster, cluster):
    """
    STR:
      1. Native MinIO storage (ConcurrentStreamParts path).
      2. Set storage.minio.partSize=130037383 (~124MiB).
      3. Load a single collection (~10GiB).
      4. For each fake CPU count (2, 8, 32, 64, 128): wrap pbm-agent with
         strace so sched_getaffinity reports that many CPUs, take a
         logical backup, sample pbm-agent VmRSS.
      5. Collect idle/peak/delta RSS per CPU count (no pass/fail on RSS).
    """
    client = pymongo.MongoClient(cluster.connection)
    cluster.check_pbm_status()
    _insert_single_collection(client, DATA_BYTES)

    n = _host("rs101")
    rows = []
    for cpus in CPU_COUNTS:
        _set_agent_fake_cpus(n, cpus)
        cluster.wait_pbm_status(wait=30)
        cluster.check_pbm_status()
        idle, peak = _logical_backup_peak_rss_kb(cluster)
        time.sleep(2)
        rows.append({
            "cpus": cpus,
            "threads": max(cpus // 2, 1),
            "idle": idle,
            "peak": peak,
            "delta": peak - idle,
        })
        Cluster.log(
            f"cpus={cpus} threads={max(cpus // 2, 1)} "
            f"idle={_pretty_bytes(idle * 1024)} peak={_pretty_bytes(peak * 1024)} "
            f"delta={_pretty_bytes((peak - idle) * 1024)}"
        )

    Cluster.log("RSS by fake CPU count:\n" + _format_rss_table(rows))
    Cluster.log("Finished successfully")
