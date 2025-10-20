import docker
import pymongo
import pytest
import yaml

from cluster import Cluster

documents = [{"_id": i, "name": f"user_{i}", "value": i * 10} for i in range(10)]
incr_docs = [{"_id": 10 + i, "name": f"user_{i}", "value": i * 10} for i in range(5)]
backup_cache = {}

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def pbm_mongodb_uri():
    return 'mongodb://pbm:pbmpass@127.0.0.1:27017/?authSource=admin&serverSelectionTimeoutMS=10000'

@pytest.fixture
def rs_encrypted_mixed_key(pbm_mongodb_uri):
    config = {
        "_id": "rs1",
        "members": [
            {"host": "rs101", "mongod_extra_args": "--enableEncryption --encryptionKeyFile=/etc/mongodb-keyfile"},
            {"host": "rs102", "mongod_extra_args": "--enableEncryption --encryptionKeyFile=/etc/mongodb-keyfile-new"},
            {"host": "rs103", "mongod_extra_args": "--enableEncryption --encryptionKeyFile=/etc/mongodb-keyfile-new"}]}
    return Cluster(config, pbm_mongodb_uri=pbm_mongodb_uri)

@pytest.fixture
def rs_encrypted_new_key(pbm_mongodb_uri):
    config = {
        "_id": "rs1",
        "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]}
    return Cluster(config, pbm_mongodb_uri=pbm_mongodb_uri, mongod_extra_args="--enableEncryption --encryptionKeyFile=/etc/mongodb-keyfile")

@pytest.fixture
def start_cluster(rs_encrypted_mixed_key, request):
    cluster = rs_encrypted_mixed_key
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        yield cluster
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(800, func_only=True)
@pytest.mark.parametrize(
    "allow_partly_done, start_new_cluster",
    [(True, False), (False, False), (False, True)],
    ids=["mixed_key_no_fb","mixed_key_fb","new_key_fb"])
@pytest.mark.parametrize('backup_type', ['physical', 'incremental'])
def test_general_PBM_T299(start_cluster, docker_client, allow_partly_done, start_new_cluster, rs_encrypted_new_key, backup_type):
    """
    Test fallback feature functionality during PBM physical restore. Due to configurations
    where database encryption keys differ between backup and running DB, restore may
    partially or fully fail, triggering fallback mechanism.
    Behavior:
    - Creates backup
    - For mixed_key_fb and new_key_fb:
        Pre-restore data is expected to persist due to fallback on restore failure
    - For mixed_key_no_fb:
        Only backup data should be present after restore (no fallback triggered)
    - Verifies that .fallbacksync directory is cleaned up after restore on all nodes
    """
    cluster = start_cluster
    fallback_expected = (allow_partly_done is False)
    cluster.check_pbm_status()
    collection = pymongo.MongoClient(cluster.connection)["test"]["test"]
    inserted_docs = []
    collection.insert_many(documents)
    inserted_docs.extend(documents)
    if backup_type == "physical":
        backup = cluster.make_backup("physical")
    elif backup_type == "incremental":
        cluster.make_backup("incremental --base")
        collection.insert_many(incr_docs)
        inserted_docs.extend(incr_docs)
        backup = cluster.make_backup("incremental")
    assert collection.delete_many({}).deleted_count == len(inserted_docs), "Document deletion failed"

    if start_new_cluster:
        cluster.destroy()
        rs_encrypted_new_key.create()
        rs_encrypted_new_key.setup_pbm()
        rs_encrypted_new_key.check_pbm_status()
        pre_restore_docs = [{"_id": 100 + i, "name": f"pre_{i}", "value": i} for i in range(10)]
        collection.insert_many(pre_restore_docs)
        rs_encrypted_new_key.make_restore(backup, timeout=500,
            restart_cluster=True, check_pbm_status=True,
            restore_opts=["--fallback-enabled=true", "--allow-partly-done=" + str(allow_partly_done).lower()])
    else:
        pre_restore_docs = [{"_id": 100 + i, "name": f"pre_{i}", "value": i} for i in range(10)]
        collection.insert_many(pre_restore_docs)
        cluster.make_restore(backup, timeout=500,
            restart_cluster=True, check_pbm_status=True,
            restore_opts=["--fallback-enabled=true", "--allow-partly-done=" + str(allow_partly_done).lower()])

    expected_docs = pre_restore_docs if fallback_expected else inserted_docs
    restored_count = collection.count_documents({})
    assert restored_count == len(expected_docs), f"Expected {len(expected_docs)} documents, found {restored_count}"
    for doc in expected_docs:
        assert collection.find_one({"_id": doc["_id"]}) == doc, f"Mismatch for _id={doc['_id']}"
    for member in cluster.config["members"]:
        host = member["host"]
        container = docker_client.containers.get(host)
        config_output = container.exec_run("cat /etc/mongod.conf")
        mongod_conf = yaml.safe_load(config_output.output.decode("utf-8"))
        db_path = mongod_conf.get("storage", {}).get("dbPath")
        assert db_path, f"Option dbPath isn't found in mongod.conf on {host}"
        check_cmd = f'sh -c "test -d {db_path}/.fallbacksync && echo exists || echo missing"'
        status = container.exec_run(check_cmd).output.decode("utf-8").strip()
        assert status == "missing", f"Dir .fallbacksync still exists on {host} at {db_path}/.fallbacksync after restore"
    Cluster.log("Finished successfully")
