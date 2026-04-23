import os
import pymongo
import pytest
from cluster import Cluster

BALLAST_COUNT = 100_000
TEST_DB = "test"
BALLAST_COLL = "ballast"
NUM_INCREMENTALS = 3
BALLAST_PAYLOAD_BYTES = 640

def _seed_ballast(client: pymongo.MongoClient) -> None:
    coll = client[TEST_DB][BALLAST_COLL]
    p = BALLAST_PAYLOAD_BYTES
    for s in range(0, BALLAST_COUNT, 100):
        e = min(s + 100, BALLAST_COUNT)
        coll.insert_many(
            [{"idx": i, "pad": os.urandom(p), "changed": -1} for i in range(s, e)],
            ordered=False)
    Cluster.log(f"Seeded {BALLAST_COUNT} {TEST_DB}.{BALLAST_COLL} ({p}B/doc)")

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
        cluster.setup_pbm()
        os.chmod("/backups", 0o777)
        os.system("rm -rf /backups/*")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(600, func_only=True)
def test_incremental_PBM_T333(start_cluster, cluster):
    client = pymongo.MongoClient(cluster.connection)
    _seed_ballast(client)
    last_name = cluster.make_backup("incremental --base")
    for i in range(NUM_INCREMENTALS):
        last_name = cluster.make_backup("incremental")
        Cluster.log(f"incr {i+1}: {last_name}")
    assert last_name is not None
    cluster.make_restore(last_name, restart_cluster=True, check_pbm_status=True)
    client = pymongo.MongoClient(cluster.connection)
    assert client[TEST_DB][BALLAST_COLL].count_documents({}) == BALLAST_COUNT
    Cluster.log("Finished successfully")