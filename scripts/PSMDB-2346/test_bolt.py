import io
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import tarfile
import time
from contextlib import contextmanager
from pathlib import Path

import docker
import pytest

from summarize import write_summary

RESULTS = Path(os.environ.get("YCSB_RESULTS_DIR", "/results"))
PSMDB_IMAGE = os.environ.get("PSMDB_IMAGE", "perconalab/percona-server-mongodb:9.0.4")
COMMUNITY_IMAGE = os.environ.get(
    "COMMUNITY_IMAGE", "mongodb/mongodb-community-server:9.0.4-ubi9"
)
YCSB_IMAGE = os.environ.get("YCSB_IMAGE", "psmdb-2346-ycsb:mongodb-labs")
# 800k records stay inside the 2 GB cache. The run count is 10x the previous
# 600k so the steady phase lasts a few minutes at the rates already measured.
RECORDCOUNT = os.environ.get("YCSB_RECORDCOUNT", "800000")
OPERATIONCOUNT = os.environ.get("YCSB_OPERATIONCOUNT", "6000000")
THREADS = os.environ.get("YCSB_THREADS", "12") or "12"
# 800k records are about 800 MB plus the index. 2 GB holds that set.
# An empty value also falls back here so mongod does not take half of RAM.
CACHE_GB = os.environ.get("YCSB_CACHE_GB", "2") or "2"
DATABASE = "ali"
# Both image entrypoints default here. The PSMDB package file /etc/mongod.conf
# says /var/lib/mongo, which is not writable by the container user.
DATA_DIR = "/data/db"
NETWORK = os.environ.get("YCSB_NETWORK", "psmdb-2346")
# topdown-fe-bound is the frontend-stall metric this CPU exposes.
# stalled-cycles-frontend is not in the kernel PMU list. perf adds a slots
# count next to topdown-fe-bound on the performance cores; frontend bound is
# topdown-fe-bound / slots.
PERF_EVENTS = (
    "instructions,cycles,L1-icache-load-misses,branches,branch-misses,"
    "iTLB-load-misses,topdown-fe-bound"
)
# Each server is measured four times. The order alternates so a steady drift
# does not land on one target.
SEQUENCES = (
    ("psmdb", "community", "community", "psmdb"),
    ("community", "psmdb", "psmdb", "community"),
)
TARGETS = {"psmdb": PSMDB_IMAGE, "community": COMMUNITY_IMAGE}


def _slots():
    step = 0
    slots = []
    for order in SEQUENCES:
        for name in order:
            step += 1
            slots.append((f"{step:02d}", name))
    return slots


def _host_results():
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        if len(fields) > 4 and fields[4] == str(RESULTS):
            return re.sub(
                r"\\([0-7]{3})",
                lambda match: chr(int(match.group(1), 8)),
                fields[3],
            )
    raise RuntimeError(f"{RESULTS} is not a bind mount, so perf cannot write its output")


def _nsenter(*args, check=True):
    result = subprocess.run(
        ["nsenter", "-t", "1", "-m", "--", *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"{' '.join(args)} failed\n{result.stdout}{result.stderr}"
        )
    return result


def _enable_perf():
    _nsenter("sysctl", "-w", "kernel.perf_event_paranoid=1")
    _nsenter("perf", "--version")


def _copy_mongod(client, image, dest):
    container = client.containers.create(image)
    try:
        stream, _ = container.get_archive("/usr/bin/mongod")
        archive = tarfile.open(fileobj=io.BytesIO(b"".join(stream)))
        member = archive.extractfile(archive.getmembers()[0])
        dest.write_bytes(member.read())
    finally:
        container.remove()


def _inspect(client, name, image):
    dest = RESULTS / name
    dest.mkdir(parents=True, exist_ok=True)
    version = client.containers.run(
        image, entrypoint=["mongod"], command=["--version"], remove=True
    )
    (dest / "mongod-version.txt").write_bytes(version)
    binary = dest / "mongod"
    _copy_mongod(client, image, binary)
    note = subprocess.run(
        ["readelf", "-p", ".note.bolt_info", str(binary)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    sections = subprocess.run(
        ["readelf", "-S", "-W", str(binary)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    interesting = "\n".join(
        line for line in sections.stdout.splitlines() if "bolt" in line or ".text" in line
    )
    (dest / "bolt-note.txt").write_text(
        "=== .note.bolt_info ===\n"
        + note.stdout
        + "\n=== executable sections ===\n"
        + interesting
        + "\n"
    )
    binary.unlink()
    _record_build_environment(client, name)


def _shell_eval(container, script):
    last = ""
    for shell in ("mongosh", "mongo"):
        try:
            result = container.exec_run([shell, "--quiet", "--eval", script])
        except docker.errors.APIError as exc:
            last = str(exc)
            if shell == "mongosh" and "not found" in last.lower():
                continue
            raise
        output = result.output.decode(errors="replace") if result.output else ""
        if result.exit_code == 0:
            return output
        missing = result.exit_code == 127 or "executable file not found" in output.lower()
        if shell == "mongosh" and missing:
            continue
        raise RuntimeError(f"{shell} failed on {container.name}\n{output}")
    raise RuntimeError(f"neither mongosh nor mongo is available on {container.name}\n{last}")


def _record_build_environment(client, name):
    container_name = f"bolt-{name}-inspect"
    print(f"=== buildInfo {name} ===", flush=True)
    container = _start_mongod(
        client, name, container_name, {"run_id": "inspect", "target": name}
    )
    try:
        output = _shell_eval(
            container,
            "print(JSON.stringify(db.adminCommand({buildInfo: 1}).buildEnvironment))",
        )
        try:
            text = json.dumps(json.loads(output.strip()), indent=2) + "\n"
        except json.JSONDecodeError:
            text = output.strip() + "\n"
        (RESULTS / name / "buildinfo.json").write_text(text)
        print(text, flush=True)
    finally:
        container.remove(force=True)


def _remove_container(client, name):
    try:
        client.containers.get(name).remove(force=True)
    except docker.errors.NotFound:
        return


def _ensure_network(client):
    try:
        return client.networks.get(NETWORK)
    except docker.errors.NotFound:
        return client.networks.create(NETWORK, driver="bridge")


def _wait_port(container):
    for _ in range(90):
        try:
            socket.create_connection((container.name, 27017), 2).close()
            return
        except OSError:
            time.sleep(1)
    logs = container.logs().decode(errors="replace")
    raise RuntimeError(f"{container.name} did not open port 27017\n{logs[-4000:]}")


def _process_name(pid):
    try:
        return Path(f"/proc/{pid}/comm").read_text().strip()
    except OSError:
        return ""


def _child_pids(pid):
    try:
        text = Path(f"/proc/{pid}/task/{pid}/children").read_text()
    except OSError:
        return []
    return [int(item) for item in text.split()]


def _mongod_command(name):
    command = [
        "mongod",
        "--dbpath", DATA_DIR,
        "--bind_ip_all",
        "--port", "27017",
    ]
    if CACHE_GB:
        command.extend(["--wiredTigerCacheSizeGB", CACHE_GB])
    if name == "psmdb":
        command.extend(["--setParameter", "perconaTelemetry=false"])
    return command


def _mongod_pid(container):
    """Host pid of the mongod process."""
    container.reload()
    root = container.attrs["State"]["Pid"]
    if not root:
        raise RuntimeError(f"{container.name} has no host pid")
    pending = [root]
    seen = set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        if _process_name(pid) == "mongod":
            return pid
        pending.extend(_child_pids(pid))
    raise RuntimeError(f"{container.name} has no mongod under host pid {root}")


@contextmanager
def _perf(pid, host_outfile, local_outfile):
    proc = subprocess.Popen(
        [
            "nsenter", "-t", "1", "-m", "--",
            "perf", "stat", "-e", PERF_EVENTS, "-p", str(pid), "-o", host_outfile, "--",
            "sleep", "infinity",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        time.sleep(0.5)
        if proc.poll() is not None:
            raise RuntimeError("perf exited before the measurement")
        yield
    finally:
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGINT)
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
        if not local_outfile.exists() or local_outfile.stat().st_size == 0:
            local_outfile.write_text("perf produced no output\n")


def _prepare_database(container):
    script = (
        f'var d = db.getSiblingDB("{DATABASE}");'
        'if (d.getCollectionNames().indexOf("usertable") < 0) d.createCollection("usertable");'
    )
    _shell_eval(container, script)


def _start_mongod(client, name, container_name, labels):
    _remove_container(client, container_name)
    container = client.containers.run(
        TARGETS[name],
        entrypoint=[],
        command=_mongod_command(name),
        name=container_name,
        detach=True,
        network=NETWORK,
        labels=labels,
    )
    _wait_port(container)
    _prepare_database(container)
    return container


def _warmup(client):
    name = SEQUENCES[0][0]
    dest = RESULTS / "warmup"
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    container_name = f"bolt-{name}-warmup"
    print(f"=== warmup {name} ===", flush=True)
    container = _start_mongod(
        client, name, container_name, {"run_id": "warmup", "target": name}
    )
    try:
        for phase in ("load", "run"):
            _ycsb(client, phase, container.name)
    finally:
        try:
            (dest / "mongod.log").write_bytes(container.logs())
        except Exception:
            pass
        container.remove(force=True)


def _ycsb(client, phase, server):
    ycsb_name = f"ycsb-{phase}-{server}"
    _remove_container(client, ycsb_name)
    print(f"starting {ycsb_name}", flush=True)
    container = client.containers.create(
        YCSB_IMAGE,
        name=ycsb_name,
        command=[
            phase, "mongodb", "-s", "-P", "workloads/workloada",
            "-p", f"recordcount={RECORDCOUNT}",
            "-p", f"operationcount={OPERATIONCOUNT}",
            "-threads", THREADS,
            "-p", f"mongodb.url=mongodb://{server}:27017/{DATABASE}",
            "-p", f"mongodb.database={DATABASE}",
        ],
        network=NETWORK,
    )
    container.start()
    status = container.wait(timeout=1800)
    logs = container.logs(stdout=True, stderr=True)
    container.remove(force=True)
    code = status.get("StatusCode", 1)
    if code != 0:
        text = logs.decode(errors="replace") if logs else ""
        raise RuntimeError(f"YCSB {phase} failed with exit {code}\n{text[-4000:]}")
    print(f"{ycsb_name} finished", flush=True)
    return logs


def _reset_run_dirs():
    warmup = RESULTS / "warmup"
    if warmup.exists():
        shutil.rmtree(warmup)
    for name in TARGETS:
        base = RESULTS / name
        if not base.is_dir():
            continue
        for path in base.iterdir():
            if path.is_dir():
                shutil.rmtree(path)


def _rows(text):
    rows = {}
    for line in text.splitlines():
        cells = [cell for cell in re.split(r"\s{2,}", line.strip()) if cell]
        if len(cells) < 2 or set(cells[0]) == {"-"}:
            continue
        rows[cells[0]] = cells[1:]
    return rows


@pytest.fixture(scope="module")
def bench():
    _enable_perf()
    client = docker.from_env(timeout=600)
    _ensure_network(client)
    RESULTS.mkdir(parents=True, exist_ok=True)
    _reset_run_dirs()
    for name, image in TARGETS.items():
        print(f"=== inspect {name} ===", flush=True)
        _inspect(client, name, image)
    _warmup(client)
    return {"client": client, "host_root": Path(_host_results())}


@pytest.fixture
def mongo(request, bench):
    run_id, name = request.param
    client = bench["client"]
    print(f"=== {run_id} {name} ===", flush=True)
    run_dir = RESULTS / name / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    container_name = f"bolt-{name}-{run_id}".lower()
    container = _start_mongod(
        client, container_name=container_name, name=name,
        labels={"run_id": run_id, "target": name},
    )
    try:
        yield container
    finally:
        try:
            (run_dir / "mongod.log").write_bytes(container.logs())
        except Exception:
            pass
        container.remove(force=True)


@pytest.mark.timeout(1800)
@pytest.mark.parametrize(
    "mongo", _slots(), indirect=True, ids=lambda slot: f"{slot[0]}-{slot[1]}"
)
def test_ycsb_slot(mongo, bench):
    run_id = mongo.labels["run_id"]
    name = mongo.labels["target"]
    run_dir = RESULTS / name / run_id
    pid = _mongod_pid(mongo)
    for phase in ("load", "run"):
        local_outfile = run_dir / f"perf-{phase}.txt"
        host_outfile = str(bench["host_root"] / name / run_id / f"perf-{phase}.txt")
        with _perf(pid, host_outfile, local_outfile):
            logs = _ycsb(bench["client"], phase, mongo.name)
        (run_dir / f"ycsb-{phase}.txt").write_bytes(logs)
        perf_text = local_outfile.read_text(errors="replace")
        if "<not counted>" in perf_text and not re.search(r"^\s*[\d,]+\s+\S+", perf_text, re.M):
            raise RuntimeError(f"perf attached to pid {pid} counted no events\n{perf_text}")
        if b"Throughput(ops/sec)" not in logs:
            raise RuntimeError(
                f"YCSB {phase} produced no throughput\n{logs.decode(errors='replace')[-4000:]}"
            )
        print(f"{run_id} {name} {phase} finished", flush=True)


def test_summary():
    text = write_summary(RESULTS)
    print(text, flush=True)
    assert "aggregate=median" in text
    rows = _rows(text)
    for label in ("load throughput (ops/sec)", "run throughput (ops/sec)"):
        assert label in rows, text
        assert rows[label][0] not in ("", "n/a"), text
        assert rows[label][1] not in ("", "n/a"), text
    for name in TARGETS:
        assert f"samples {name}=4" in text
    for event in PERF_EVENTS.split(","):
        label = f"perf run {event}"
        assert label in rows, text
        assert rows[label][0] not in ("", "n/a"), text
        assert rows[label][1] not in ("", "n/a"), text
    for label in (
        "perf run ipc",
        "perf run L1i misses / 1k instr",
        "perf run iTLB misses / 1k instr",
        "perf run branch-miss rate %",
        "perf run frontend bound %",
        "run READ p99 (us)",
        "run UPDATE p99 (us)",
    ):
        assert label in rows, text
        assert rows[label][0] not in ("", "n/a"), text
        assert rows[label][1] not in ("", "n/a"), text
