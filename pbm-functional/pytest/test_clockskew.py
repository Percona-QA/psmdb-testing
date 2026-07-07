import concurrent.futures
import json
import os
import random
import pymongo
import pymongo.errors
import pytest
import testinfra

from cluster import Cluster


@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {
            "_id": "rscfg",
            "members": [{"host": "rscfg01"}, {"host": "rscfg02"}, {"host": "rscfg03"}],
        },
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}]},
        ],
    }

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
        cluster.setup_pbm("/etc/pbm-fs.conf")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def apply_clock_skew(hosts, shift):
    """For each host: installs a mongod wrapper (so mongod subprocesses don't inherit the fake time),
    tells the pbm-agent to run with a shifted clock, and then reloads the agent"""
    for host in hosts:
        n = testinfra.get_host(f"docker://{host}")
        # Wrap the mongod binary so pbm-agent's LD_PRELOAD doesn't bleed into mongod subprocesses.
        # mongod hangs on startup when libfaketime is loaded via LD_PRELOAD.
        n.check_output("mv /usr/bin/mongod /usr/bin/mongod.real")
        n.check_output("echo '#!/bin/sh' > /usr/bin/mongod")
        n.check_output("echo 'exec env -u LD_PRELOAD /usr/bin/mongod.real \"$@\"' >> /usr/bin/mongod")
        n.check_output("chmod +x /usr/bin/mongod")
        n.check_output(
            f"sed -i 's|^environment=\\(.*\\)|environment=\\1,LD_PRELOAD=\"/usr/lib64/libfaketime.so.1\",FAKETIME=\"{shift}\"|' "
            "/etc/supervisord.d/pbm-agent.ini"
        )
        n.check_output("supervisorctl reread")
        n.check_output("supervisorctl update pbm-agent")

def run_clock_skew_test(cluster, backup_type, restore_timeout):
    CLOCK_SHIFTS = ["+90m", "-195m", "+2d", "-7h", "+11m", "+42d", "-13h"]

    for shard in cluster.config["shards"]:
        shift = random.choice(CLOCK_SHIFTS)
        shard_hosts = [m["host"] for m in shard["members"]]
        Cluster.log(f"Applying clock skew {shift!r} to shard {shard['_id']} hosts {shard_hosts}")
        apply_clock_skew(shard_hosts, shift)

    cluster.wait_pbm_status(wait=30)
    cluster.check_pbm_status()

    background_inserts_running = True

    def background_insert(rs_id, primary):
        Cluster.log(f"Starting background inserts to shard {rs_id} via {primary}")
        shard_client = pymongo.MongoClient(
            f"mongodb://root:root@{primary}:27017/?replicaSet={rs_id}&authSource=admin"
        )
        collection = shard_client["test"][f"bounds_{rs_id}"]
        counter = 0
        while background_inserts_running:
            try:
                collection.insert_one({"seq": counter})
                counter += 1
            except pymongo.errors.PyMongoError as exc:
                Cluster.log(f"{rs_id}: {exc}")
                continue
        shard_client.close()
        Cluster.log(f"Stopping background inserts to shard {rs_id}, wrote {counter} docs")
        return counter

    executor = concurrent.futures.ThreadPoolExecutor()
    futures = {
        shard["_id"]: executor.submit(background_insert, shard["_id"], shard["members"][0]["host"])
        for shard in cluster.config["shards"]
    }

    try:
        backup = cluster.make_backup(backup_type)
    except AssertionError as e:
        background_inserts_running = False
        for f in futures.values():
            f.result()
        assert False, e

    background_inserts_running = False
    totals = {rs_id: future.result() for rs_id, future in futures.items()}

    backup_meta = json.loads(
        testinfra.get_host("docker://rscfg01").check_output(f"cat /backups/{backup}.pbm.json")
    )
    Cluster.log(json.dumps(backup_meta, indent=4))

    restart_cluster = backup_type == "physical"
    cluster.make_restore(backup, restart_cluster=restart_cluster, check_pbm_status=True, timeout=restore_timeout)

    for shard in cluster.config["shards"]:
        rs_id = shard["_id"]
        rs_primary = shard["members"][0]["host"]
        total_inserted = totals[rs_id]

        client = pymongo.MongoClient(
            f"mongodb://root:root@{rs_primary}:27017/?replicaSet={rs_id}&authSource=admin"
        )
        restored = sorted(d["seq"] for d in client["test"][f"bounds_{rs_id}"].find())
        client.close()

        Cluster.log(f"{rs_id}: {len(restored)}/{total_inserted} docs restored")

        assert restored == list(range(len(restored))), (
            f"{rs_id}: gap in restored sequence; got {restored[:20]}..."
        )
        # Docs beyond the restored window must be absent.
        restored_set = set(restored)
        for seq in range(len(restored), total_inserted):
            assert seq not in restored_set, (
                f"{rs_id}: seq={seq} should be absent (inserted after backup cutoff) but is present"
            )

    Cluster.log("Finished successfully\n")


@pytest.mark.timeout(600, func_only=True)
def test_clock_skew_logical_PBM_T354(start_cluster, cluster):
    run_clock_skew_test(cluster, "logical", restore_timeout=240)


@pytest.mark.timeout(3600, func_only=True)
def test_clock_skew_physical_PBM_T355(start_cluster, cluster):
    run_clock_skew_test(cluster, "physical", restore_timeout=1200)
