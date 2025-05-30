import pytest
import pymongo
import os
import docker
import threading
import time

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
def test_logical_PBM_T294(start_cluster, cluster):
    """
    Test case for to check multipart upload of backup files with custom maxUploadParts option.
    It checks that file is divided with proper part size allowing file to grow during backup.
    """
    cluster.exec_pbm_cli("config --set storage.s3.maxUploadParts=5")
    client = pymongo.MongoClient(cluster.connection)
    collection = client["test"]["bigdata"]
    doc_size_kb = 10
    total_size_mb = 1000
    total_docs = (total_size_mb * 1024) // doc_size_kb
    batch_size = 1000
    docs_before = total_docs // 3
    docs_during = total_docs - docs_before
    def insert_random_data(start_id, count):
        Cluster.log(f"Inserting {count} documents")
        for i in range(start_id, start_id + count, batch_size):
            batch = []
            for j in range(min(batch_size, (start_id + count) - i)):
                random_data = os.urandom(doc_size_kb * 1024).hex()
                batch.append({"_id": i + j, "payload": random_data})
            collection.insert_many(batch)
            time.sleep(0.1)
        Cluster.log(f"Inserted {count} documents")
    insert_random_data(start_id=0, count=docs_before)
    insert_thread = threading.Thread(target=insert_random_data, args=(docs_before, docs_during))
    insert_thread.start()
    Cluster.log("Starting backup")
    result = cluster.exec_pbm_cli("backup --wait")
    insert_thread.join()
    assert result.rc == 0, f"PBM backup failed\nstderr:\n{result.stderr}"
    logs=cluster.exec_pbm_cli("logs -sD -t0 -e backup")
    error_lines = [line for line in logs.stdout.splitlines() if " E " in line and "active lock is present" not in line]
    if error_lines:
        error_summary = "\n".join(error_lines)
        raise AssertionError(f"Errors found in PBM backup logs\n{error_summary}")
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300, func_only=True)
def test_logical_PBM_T295(start_cluster, cluster):
    """
    Test case for to check multipart upload of oplog with custom maxUploadParts option
    """
    cluster.exec_pbm_cli("config --set storage.s3.maxUploadParts=5")
    client = pymongo.MongoClient(cluster.connection)
    collection = client["test"]["bigdata"]
    doc_size_kb = 10
    total_size_mb = 1000
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
            time.sleep(0.1)
        Cluster.log(f"Inserted {count} documents")
    insert_thread = threading.Thread(target=insert_random_data, args=(total_docs,))
    insert_thread.start()
    Cluster.log("Starting backup")
    result = cluster.exec_pbm_cli("backup --ns=random.* --wait")
    insert_thread.join()
    assert result.rc == 0, f"PBM backup failed\nstderr:\n{result.stderr}"
    logs = cluster.exec_pbm_cli("logs -sD -t0 -e backup")
    error_lines = [line for line in logs.stdout.splitlines() if " E " in line and "active lock is present" not in line]
    if error_lines:
        error_summary = "\n".join(error_lines)
        raise AssertionError(f"Errors found in PBM backup logs\n{error_summary}")
    Cluster.log("Finished successfully")
