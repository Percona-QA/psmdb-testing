"""
PBM-1554: UnrecoverableRollbackError / fassert 31049 after incremental restore.

Caught by operator e2e demand-backup-incremental-azure on a simple 3-node RS:
  incremental-base -> restore -> STS recreate (OrderedReady).

cluster.py gives rs101 priority 1000, so once a majority is up during OrderedReady
(rs101 then rs102) it becomes PRIMARY. If it takes w:1 writes that peers have not
applied, then goes down and later re-joins as SECONDARY, RTS runs with stableTS
still unset after takeUnstableCheckpointOnShutdown -> fassert 31049.
"""

import time

import docker
import pymongo
import pytest
from bson.timestamp import Timestamp

from cluster import Cluster

DOCUMENTS = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]
# cluster.py: members[0] -> priority 1000
PRIMARY_CANDIDATE = "rs101"
SECONDARIES = ["rs102", "rs103"]
DOCKER = docker.from_env()


@pytest.fixture(scope="package")
def config():
    return {
        "_id": "rs1",
        "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}],
    }


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(
        config,
        mongod_extra_args=(
            " --enableEncryption --encryptionKeyFile=/etc/mongodb-keyfile"
            " --setParameter enableTestCommands=1"
            " --setParameter=logicalSessionRefreshMillis=10000"
            " --setParameter=shutdownTimeoutMillisForSignaledShutdown=300"
        ),
    )


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)


@pytest.mark.timeout(900, func_only=True)
def test_STR_incremental_restore_bringup(start_cluster, cluster):
    cluster.check_pbm_status()
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many(DOCUMENTS)

    backup = cluster.make_backup("incremental --base")
    client["test"]["test"].drop()

    # Physical restore leaves mongod down on every member.
    cluster.make_restore(
        backup,
        restart_cluster=False,
        check_pbm_status=False,
        make_resync=False,
    )

    def direct(host):
        return pymongo.MongoClient(
            f"mongodb://root:root@{host}:27017/"
            "?authSource=admin&directConnection=true",
            serverSelectionTimeoutMS=5000,
        )

    def wait_ping(host, timeout=60):
        # May never come up if mongod already hit fassert 31049.
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                direct(host).admin.command("ping")
                return True
            except Exception:
                time.sleep(0.2)
        return False

    def failpoint(host, name, mode="alwaysOn", data=None):
        cmd = {"configureFailPoint": name, "mode": mode}
        if data is not None:
            cmd["data"] = data
        direct(host).admin.command(cmd)

    def pin_stable_ts(host):
        # Keep post-restore unstable checkpoint: RTS has nothing to recover to.
        failpoint(
            host,
            "holdStableTimestampAtSpecificTimestamp",
            data={"timestamp": Timestamp(0, 0)},
        )

    # OrderedReady: pod-0 first. Alone -> no majority, stays secondary.
    # Container restart matches operator STS/pod recreate better than supervisorctl.
    DOCKER.containers.get(PRIMARY_CANDIDATE).restart()
    if wait_ping(PRIMARY_CANDIDATE):
        pin_stable_ts(PRIMARY_CANDIDATE)

    # pod-1: majority with rs101. Priority 1000 -> rs101 becomes PRIMARY.
    DOCKER.containers.get(SECONDARIES[0]).restart()
    if wait_ping(SECONDARIES[0]):
        failpoint(SECONDARIES[0], "stopReplProducer")
        pin_stable_ts(SECONDARIES[0])

    deadline = time.time() + 120
    primary = None
    while time.time() < deadline:
        for host in (PRIMARY_CANDIDATE, SECONDARIES[0]):
            try:
                hello = direct(host).admin.command("hello")
                if hello.get("isWritablePrimary") or hello.get("ismaster"):
                    primary = host
                    break
            except Exception:
                pass
        if primary:
            break
        time.sleep(1)
    assert primary == PRIMARY_CANDIDATE, f"expected {PRIMARY_CANDIDATE} primary, got {primary}"

    # pod-2: still not applying (frozen fetch) while PRIMARY can advance alone.
    DOCKER.containers.get(SECONDARIES[1]).restart()
    if wait_ping(SECONDARIES[1]):
        failpoint(SECONDARIES[1], "stopReplProducer")
        pin_stable_ts(SECONDARIES[1])

    # Private tip on the priority PRIMARY only (peers not fetching).
    # PBM meta writes use w:majority, so a plain w:1 insert is used here instead.
    direct(PRIMARY_CANDIDATE)["test"]["test"].with_options(
        write_concern=pymongo.WriteConcern(w=1)
    ).insert_one({"str": "post-restore-only-on-primary"})

    # PRIMARY -> down (pod stop). Peers must elect on the shared restore tip.
    DOCKER.containers.get(PRIMARY_CANDIDATE).stop()
    for host in SECONDARIES:
        failpoint(host, "stopReplProducer", mode="off")

    deadline = time.time() + 120
    new_primary = None
    while time.time() < deadline:
        for host in SECONDARIES:
            try:
                hello = direct(host).admin.command("hello")
                if hello.get("isWritablePrimary") or hello.get("ismaster"):
                    new_primary = host
                    break
            except Exception:
                pass
        if new_primary:
            break
        time.sleep(1)
    assert new_primary is not None, f"no primary among {SECONDARIES}"
    Cluster.log(f"{PRIMARY_CANDIDATE} was PRIMARY; now {new_primary} after step-down window")

    # Former PRIMARY re-joins as SECONDARY (pod start) and must RTS its private tip.
    DOCKER.containers.get(PRIMARY_CANDIDATE).start()
    if wait_ping(PRIMARY_CANDIDATE):
        pin_stable_ts(PRIMARY_CANDIDATE)

    deadline = time.time() + 180
    crashed = None
    while time.time() < deadline:
        logs = (
            DOCKER.containers.get(PRIMARY_CANDIDATE)
            .logs(tail=8000)
            .decode("utf-8", errors="replace")
        )
        if (
            "UnrecoverableRollbackError" in logs
            or "No stable timestamp available to recover to" in logs
            or ("Fatal assertion" in logs and "31049" in logs)
        ):
            crashed = PRIMARY_CANDIDATE
            break
        time.sleep(2)

    assert crashed is not None, (
        f"Expected UnrecoverableRollbackError / fassert 31049 on {PRIMARY_CANDIDATE} "
        f"after PRIMARY->SECONDARY re-join (new primary was {new_primary})"
    )
    Cluster.log(f"Reproduced PBM-1554 on {crashed}")
