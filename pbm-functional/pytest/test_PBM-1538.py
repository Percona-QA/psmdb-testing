import pytest
import pymongo
import os
import docker
import threading
import time
import json

from cluster import Cluster

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host":"rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        os.chmod("/backups",0o777)
        os.system("rm -rf /backups/*")
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300, func_only=True)
def test_logical_PBM_T298(start_cluster, cluster):
    """
    Test case to check that backup is NOT marked as successful if oplog wasn't uploaded
    """
    result = cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio:15380 --wait")
    time.sleep(5)
    client = pymongo.MongoClient(cluster.connection)
    collection = client["test"]["bigdata"]
    doc_size_kb = 10
    total_size_mb = 200
    total_docs = (total_size_mb * 1024) // doc_size_kb
    batch_size = 1000
    def insert_random_data(count):
        Cluster.log(f"Inserting {count} documents")
        for i in range(0, count, batch_size):
            batch = []
            for j in range(min(batch_size, count - i)):
                random_data = os.urandom(doc_size_kb * 1024).hex()
                batch.append({"_id": i + j, "payload": random_data})
            collection.insert_many(batch)
            time.sleep(0.2)
        Cluster.log(f"Inserted {count} documents")
    insert_thread = threading.Thread(target=insert_random_data, args=(total_docs,))
    insert_thread.start()
    result = cluster.exec_pbm_cli("backup --out json")
    try:
        backup_info = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise AssertionError(f"Failed to get backup name: {result.stdout}")
    backup_name = backup_info.get("name")
    insert_thread.join()
    result = cluster.exec_pbm_cli("status -s backups --out json")
    try:
        backup_info = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise AssertionError(f"Failed to parse backup info: {result.stdout}")
    for snapshot in backup_info.get("backups", {}).get("snapshot", []):
        if snapshot.get("name") == backup_name:
            backup_status = snapshot.get("status")
            break
    logs = cluster.exec_pbm_cli("logs -sD -t0 -e backup")
    upload_errors = [line for line in logs.stdout.splitlines() if "failed to upload oplog" in line]
    if backup_status == "done":
        if upload_errors:
            error_summary = "\n".join(upload_errors)
            raise AssertionError(f"Backup succeeded, but errors found in PBM logs: {error_summary}")
        else:
            Cluster.log("Backup completed successfully without oplog upload errors")
    elif backup_status == "error":
        Cluster.log("Backup failed as expected")