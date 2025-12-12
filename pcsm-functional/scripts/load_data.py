import os
import random
import pymongo
import base64
import time
import signal
import threading
from bson import ObjectId, BSON
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

shutdown_event = threading.Event()

def parse_args():
    parser = argparse.ArgumentParser(description="Load test data into MongoDB")
    parser.add_argument(
        "--port",
        type=int,
        default=27017,
        help="MongoDB port to connect to (default: 27017)",
    )
    return parser.parse_args()

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def handle_shutdown(signum, frame):
    log(f"Received signal {signum}, initiating shutdown")
    shutdown_event.set()

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def generate_distributed_counts(total, chunks, variation=0.4):
    base = total // chunks
    counts = []
    for _ in range(chunks):
        delta = int(base * variation)
        count = random.randint(base - delta, base + delta)
        counts.append(count)
    diff = total - sum(counts)
    for i in range(abs(diff)):
        counts[i % chunks] += (1 if diff > 0 else -1)
    return counts

def generate_entropy_pool(num_blocks=20, block_size=100_000):
    return [
        base64.b64encode(os.urandom(block_size)).decode('ascii')
        for _ in range(num_blocks)]

def get_template_doc(args):
    template_type, pool = args
    if template_type == "random" and pool:
        p1 = random.choice(pool)
        p2 = random.choice(pool)
        return {
            "_id": ObjectId(),
            "int": 42,
            "float": 3.14159,
            "string": base64.b64encode(os.urandom(100)).decode('ascii'),
            "padding1": p1,
            "padding2": p2,
            "array": list(range(40))
        }
    else:
        return {
            "_id": ObjectId(),
            "int": 42,
            "float": 3.14159,
            "string": "x" * 100,
            "padding1": "a" * 100_000,
            "padding2": "b" * 100_000,
            "array": [1] * 40
        }

def estimate_doc_size(template):
    return len(BSON.encode(template))

def insert_documents(collection, docs):
    try:
        collection.insert_many(docs, ordered=False, bypass_document_validation=True)
    except Exception as e:
        return str(e)
    return None

def collection_worker(collection_name, count, template_type, pool_data, db_name, port):
    try:
        client = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}")
        db = client[db_name]
        collection = db[collection_name]
        batch_size = 1000
        inserted = 0
        while inserted < count and not shutdown_event.is_set():
            current_batch = min(batch_size, count - inserted)
            docs = [get_template_doc((template_type, pool_data)) for _ in range(current_batch)]
            error = insert_documents(collection, docs)
            if error:
                log(f"[{collection_name}] Error: {error}")
                break
            inserted += current_batch
    except Exception as e:
        log(f"[{collection_name}] Error: {e}")

def enable_and_shard_all_collections(port):
    db_name = os.getenv("DBNAME", "test_db")

    client = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}")
    admin = client["admin"]
    db = client[db_name]

    admin.command("enableSharding", db_name)

    for coll_name in db.list_collection_names():
        if coll_name.startswith("system."):
            continue

        ns = f"{db_name}.{coll_name}"
        log(f"Sharding collection {ns}")

        try:
            admin.command(
                "shardCollection",
                ns,
                key={"_id": 1},
            )
        except Exception as e:
            log(f"Failed to shard {ns}: {str(e)}")

def load_data(port):
    start_time = time.time()

    total_collections = int(os.getenv("COLLECTIONS", 5))
    datasize_mb = int(os.getenv("DATASIZE", 1024))
    distribute = os.getenv("DISTRIBUTE", "false").lower() == "true"
    template_type = os.getenv("DOC_TEMPLATE", "compressible").lower()
    db_name = os.getenv("DBNAME", "test_db")
    env_threads = os.getenv("THREADS")
    max_threads = int(env_threads) if env_threads else min(4, int((os.cpu_count() or 1) * 0.75))

    pool_data = generate_entropy_pool() if template_type == "random" else None
    sample_doc = get_template_doc((template_type, pool_data))
    estimated_doc_size = estimate_doc_size(sample_doc)
    total_bytes = datasize_mb * 1024 * 1024
    total_docs = total_bytes // estimated_doc_size

    log(f"Starting data generation: database: {db_name}, {total_collections} collections, ~{datasize_mb} MB")
    log(f"Template: {template_type}, threads: {max_threads}, distribute: {distribute}")
    log(f"Estimated doc size: {estimated_doc_size} bytes")
    log(f"Total documents to insert: {total_docs}")

    if distribute:
        doc_counts = generate_distributed_counts(total_docs, total_collections, variation=0.4)
    else:
        base = total_docs // total_collections
        remainder = total_docs % total_collections
        doc_counts = [base + 1 if i < remainder else base for i in range(total_collections)]

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(collection_worker, f"collection{i}", count, template_type, pool_data, db_name, port)
            for i, count in enumerate(doc_counts)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"[Error] Worker thread failed: {e}")
    elapsed = time.time() - start_time
    log(f"Data generation finished in {elapsed:.2f} seconds")

if __name__ == "__main__":
    args = parse_args()
    load_data(args.port)
    enable_and_shard_all_collections(args.port)
