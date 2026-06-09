import json

import pymongo
import pytest
import testinfra

from cluster import Cluster

CONFIGSERVER_URI = "mongodb://pbm:pbmpass@rscfg01:27017/?authSource=admin"
MONGOS_URI = "mongodb://pbm:pbmpass@mongos:27017/?authSource=admin"


@pytest.fixture(scope="package")
def config():
    return {
        "mongos": "mongos",
        "configserver": {"_id": "rscfg", "members": [{"host": "rscfg01"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
        ],
    }

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy(cleanup_backups=True)
        cluster.create()
        cluster.setup_pbm()
        client = pymongo.MongoClient(cluster.connection)
        client["test"]["data"].insert_many([{"x": i} for i in range(100)])
        client.close()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

def _find_snapshot(status_json, backup_name):
    try:
        data = json.loads(status_json)
        for snapshot in data.get("backups", {}).get("snapshot", []):
            if snapshot.get("name") == backup_name:
                return snapshot
    except (json.JSONDecodeError, KeyError):
        pass
    return None

@pytest.mark.timeout(300, func_only=True)
def test_backup_status_consistent_across_uris_PBM_1675(start_cluster, cluster):
    n = testinfra.get_host("docker://rscfg01")

    backup_result = n.run(f'pbm backup --mongodb-uri="{MONGOS_URI}" --wait --out=json')
    assert backup_result.rc == 0, f"Backup failed: {backup_result.stdout}\n{backup_result.stderr}"
    backup_name = json.loads(backup_result.stdout).get("name")
    assert backup_name, f"Could not parse backup name from: {backup_result.stdout}"

    status_mongos = n.run(f'pbm status --mongodb-uri="{MONGOS_URI}" --out=json')
    assert status_mongos.rc == 0, f"pbm status via mongos failed: {status_mongos.stderr}"
    mongos_snapshot = _find_snapshot(status_mongos.stdout, backup_name)
    assert mongos_snapshot, f"Backup '{backup_name}' not found in pbm status via mongos"
    assert mongos_snapshot.get("status") == "done", (
        f"'done' status via mongos not found, got '{mongos_snapshot.get('status')}'"
    )

    status_configserver = n.run(f'pbm status --mongodb-uri="{CONFIGSERVER_URI}" --out=json')
    assert status_configserver.rc == 0, f"pbm status via config server failed: {status_configserver.stderr}"
    configserver_snapshot = _find_snapshot(status_configserver.stdout, backup_name)
    assert configserver_snapshot, f"Backup '{backup_name}' not found in pbm status via config server"
    assert configserver_snapshot.get("status") == "done", (
        f"'done' status not found, got {configserver_snapshot.get('status')}"
    )
