import pytest
import pymongo
import pymongo.encryption
import bson
import testinfra
import time
import os
import docker

from datetime import datetime
from cluster import Cluster

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def config():
    return { "_id": "rs1", "members": [{"host":"rs101"},{"host": "rs102"},{"host": "rs103" }]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster,request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300,func_only=True)
def test_logical(start_cluster,cluster):
    cluster.check_pbm_status()
    local_master_key = os.urandom(96)
    
    # Use local KMS key
    kms_providers = {"local": {"key": local_master_key}}

    client = pymongo.MongoClient(cluster.connection)
    coll = client.test.test

    # Create keyVault collection
    client["enc"]["test"].create_index(
        "keyAltNames",
        unique=True,
        partialFilterExpression={"keyAltNames": {"$exists": True}},
    )

    # Create a clientEncryption instance
    client_encryption = pymongo.encryption.ClientEncryption(
        kms_providers,
        "enc.test",
        client,
        coll.codec_options,
    )

    # Create a new data key for the encryptedField.
    data_key_id = client_encryption.create_data_key(
        "local", key_alt_names=["test"]
    )

    # Explicitly encrypt a field:
    encrypted_field = client_encryption.encrypt(
        "encrypted_field",
        pymongo.encryption.Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
        key_id=data_key_id,
    )

    # Insert a doc with both encrypted and unencrypted fields
    coll.insert_one({"encryptedField": encrypted_field, "unencryptedField": "unencrypted_field"})

    # Perform a physical backup
    backup=cluster.make_backup("physical")

    # Close connections
    client_encryption.close()
    client.close()

    # Drop key database
    pymongo.MongoClient(cluster.connection).drop_database('enc')

    # Make restore
    cluster.make_restore(backup,restart_cluster=True, check_pbm_status=True)

    # Reconnect 
    client = pymongo.MongoClient(cluster.connection)
    coll = client.test.test
    client_encryption = pymongo.encryption.ClientEncryption(
        kms_providers,
        "enc.test",
        client,
        coll.codec_options,
    )

    # fetch the test doc and check
    doc = coll.find_one()
    Cluster.log("Encrypted document: %s" % (doc,))
    assert doc["unencryptedField"] == "unencrypted_field"
    assert doc["encryptedField"] != "encrypted_field"
    assert client_encryption.decrypt(doc["encryptedField"]) == "encrypted_field"
