import pymongo
import threading
import time
from bson import ObjectId

from cluster import Cluster
from data_types.basic_collection_types import create_collection_types, perform_crud_ops_collection
from data_types.index_types import create_index_types
from data_types.extended_collection_types import create_diff_coll_types
from data_types.sharded_collection_types import create_sharded_collection_types, perform_crud_ops_sharded_collection
from data_types.sharded_index_types import create_sharded_index_types

# Set no_shard_key to False due to PCSM-220
DEFAULT_NO_SHARD_KEY = False
# Set update_shard_key to False due to PCSM-221
DEFAULT_UPDATE_SHARD_KEY = False

stop_operations_map = {}

def create_all_types_db(connection_string, db_name="init_test_db", create_ts=False, drop_before_creation=False,
                        start_crud=False, is_sharded=False, no_shard_key=None, update_shard_key=None):
    if no_shard_key is None:
        no_shard_key = DEFAULT_NO_SHARD_KEY
    if update_shard_key is None:
        update_shard_key = DEFAULT_UPDATE_SHARD_KEY

    client = pymongo.MongoClient(connection_string)
    db = client[db_name]

    collection_metadata = create_collection_types(db, create_ts, drop_before_creation)
    create_index_types(db, drop_before_creation)
    create_diff_coll_types(db, drop_before_creation)

    if is_sharded:
        sharded_collection_metadata = create_sharded_collection_types(db, create_ts, drop_before_creation)
        create_sharded_index_types(db, drop_before_creation)
        collection_metadata.extend(sharded_collection_metadata)

    if start_crud:
        if db_name not in stop_operations_map:
            stop_operations_map[db_name] = threading.Event()

        stop_operations_map[db_name].clear()

        operation_thread = threading.Thread(
            target=continuous_crud_ops_collection_background,
            args=(collection_metadata, stop_operations_map[db_name], no_shard_key, update_shard_key)
        )
        operation_thread.start()
        return db, [operation_thread]

    return db, []

def continuous_crud_ops_collection_background(collection_metadata, stop_event, no_shard_key, update_shard_key):
    while not stop_event.is_set():
        for metadata in collection_metadata:
            try:
                if metadata.get("sharded"):
                    shard_key = metadata.get("shard_key")
                    hashed = metadata.get("hashed")
                    perform_crud_ops_sharded_collection(metadata["collection"], shard_key, hashed, no_shard_key, update_shard_key)
                else:
                    perform_crud_ops_collection(metadata["collection"], metadata["capped"], metadata["timeseries"])
            except Exception:
                time.sleep(0.1)
        time.sleep(0.1)

def stop_db_crud_operations(db_name):
    if db_name in stop_operations_map:
        stop_operations_map[db_name].set()

def stop_all_crud_operations():
    for db_name, stop_event in stop_operations_map.items():
        stop_event.set()

def generate_dummy_data(connection_string, db_name="dummy", num_collections=5, doc_size=150000,
                        batch_size=10000, stop_event=None, sleep_between_batches=0, drop_before_creation=True,
                        is_sharded=False):
    """
    With default parameters generates ~500MB of data within 10 seconds
    If stop_event is provided, it can be used to stop generation early.
    """

    Cluster.log("Generating dummy data...")
    client = pymongo.MongoClient(connection_string)
    db = client[db_name]

    if drop_before_creation:
        client.drop_database(db_name)

    if is_sharded:
        try:
            client.admin.command("enableSharding", db_name)
        except pymongo.errors.OperationFailure:
            pass

    collections = [f"collection_{i}" for i in range(num_collections)]

    template_doc = {
        "int": 42,
        "float": 3.14159,
        "string": "x" * 100,
        "padding1": "a" * 500,
        "padding2": "b" * 200,
        "array": [1] * 40,
    }

    for coll_name in collections:
        if stop_event and stop_event.is_set():
            break
        collection = db[coll_name]

        if is_sharded:
            try:
                client.admin.command("shardCollection", f"{db_name}.{coll_name}", key={"_id": "hashed"})
            except pymongo.errors.OperationFailure:
                pass

        for _ in range(doc_size // batch_size):
            if stop_event and stop_event.is_set():
                break
            docs = [{**template_doc, "_id": ObjectId()} for _ in range(batch_size)]
            collection.insert_many(docs, ordered=False, bypass_document_validation=True)
            if sleep_between_batches > 0:
                time.sleep(sleep_between_batches)
    Cluster.log("Dummy data generation is completed")