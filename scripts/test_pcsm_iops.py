import pymongo
import time
import argparse
import os
import signal
import threading
from bson import ObjectId, BSON

DB_NAME = "testdb"
COLLECTION_PREFIX = "testcol"
MAX_COLLECTION_BYTES = 5_000_000_000  # 5 GB

TEMPLATES = {
    "small":   {"size": 500,     "batch": 2000},
    "regular": {"size": 5_000,   "batch": 500},
    "big":     {"size": 100_000, "batch": 100},
    "huge":    {"size": 200_000, "batch": 50},
}

total_inserted = 0
start_time = time.time()
running = True
lock = threading.Lock()
threads = []


def make_padding(size, compressible):
    if compressible:
        chunk = b"AAAA" * 64  # 256-byte repeating block
        return (chunk * (size // len(chunk) + 1))[:size]
    return os.urandom(size)


def build_pool(template, compressible, batch_size):
    target = TEMPLATES[template]["size"]
    overhead = len(BSON.encode({
        "_id": ObjectId(),
        "i": 0,
        "f": 3.14,
        "s": "",
        "p": b"",
    }))
    pad_size = max(0, target - overhead)
    pool_size = max(batch_size, 500)

    pool = []
    for _ in range(pool_size):
        doc = {
            "_id": ObjectId(),
            "i": 0,
            "f": 3.14,
            "s": "bench",
            "p": make_padding(pad_size, compressible),
        }
        pool.append(doc)

    actual = len(BSON.encode(pool[0]))
    print(f"[Pool] Pre-generated {pool_size} docs, each ~{actual} B ({actual/1024:.1f} KB)")
    return pool, actual


def refresh_batch(pool, batch_size):
    batch = []
    pool_size = len(pool)
    for i in range(batch_size):
        doc = pool[i % pool_size].copy()
        doc["_id"] = ObjectId()
        batch.append(doc)
    return batch


def signal_handler(sig, frame):
    global running
    running = False
    print("\n[Interrupt received] Shutting down...")


def is_sharded_cluster(client):
    try:
        result = client.admin.command("hello")
        return result.get("msg") == "isdbgrid"
    except Exception:
        try:
            result = client.admin.command("isMaster")
            return result.get("msg") == "isdbgrid"
        except Exception:
            return False


def enable_sharding(client, db_name):
    admin_db = client.admin
    try:
        admin_db.command("enableSharding", db_name)
        print(f"[Sharding] Enabled sharding on database '{db_name}'")
    except Exception as e:
        msg = str(e).lower()
        if "already enabled" in msg or "already sharded" in msg:
            print(f"[Sharding] Database '{db_name}' is already sharded")
        else:
            print(f"[Sharding] Warning: Could not enable sharding: {e}")


def shard_collection(client, db_name, collection_name, shard_key=None):
    if shard_key is None:
        shard_key = {"_id": "hashed"}
    admin_db = client.admin
    namespace = f"{db_name}.{collection_name}"
    try:
        admin_db.command("shardCollection", namespace, key=shard_key)
        print(f"[Sharding] Sharded collection '{namespace}' with key {shard_key}")
    except Exception as e:
        msg = str(e).lower()
        if "already sharded" in msg or "already exists" in msg:
            print(f"[Sharding] Collection '{namespace}' is already sharded")
        else:
            print(f"[Sharding] Warning: Could not shard '{namespace}': {e}")


def reporter_thread(interval=5):
    global total_inserted, start_time
    last_counter = 0
    while running:
        time.sleep(interval)
        with lock:
            now = time.time()
            elapsed = now - start_time
            count = total_inserted
        delta = count - last_counter
        iops = delta / interval
        avg = count / elapsed if elapsed > 0 else 0
        print(f"[{elapsed:.1f}s] Inserts: {count} | IOPS (last {interval}s): {iops:.0f} | Avg IOPS: {avg:.0f}")
        last_counter = count


def writer_thread(client, collection_name, per_coll_ops, pool, doc_size, batch_size, sharded):
    global total_inserted
    db = client[DB_NAME]
    collection = db[collection_name]
    batch_interval = batch_size / per_coll_ops
    next_batch_time = time.time()
    inserted_in_collection = 0

    while running:
        estimated_size = inserted_in_collection * doc_size
        if estimated_size >= MAX_COLLECTION_BYTES:
            print(f"[Info] Dropping '{collection_name}' (estimated ~{estimated_size / 1024**2:.0f} MB)")
            collection.drop()
            collection = db[collection_name]
            if sharded:
                shard_collection(client, DB_NAME, collection_name)
            inserted_in_collection = 0

        batch = refresh_batch(pool, batch_size)
        collection.insert_many(batch, ordered=False)
        inserted_in_collection += batch_size

        with lock:
            total_inserted += batch_size

        next_batch_time += batch_interval
        sleep_time = next_batch_time - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            next_batch_time = time.time()


def main():
    global start_time
    parser = argparse.ArgumentParser(description="MongoDB insert load generator")
    parser.add_argument("--ops", type=int, default=100, help="Target inserts per second (default: 100)")
    parser.add_argument("--collections", type=int, default=1, help="Number of collections (default: 1)")
    parser.add_argument("-u", "--user", type=str, default=None, help="MongoDB username")
    parser.add_argument("-p", "--password", type=str, default=None, help="MongoDB password")
    parser.add_argument("--host", type=str, default="localhost:27017", help="MongoDB host (default: localhost:27017)")
    parser.add_argument("--template", type=str, default="regular",
                        choices=list(TEMPLATES.keys()),
                        help="Document size template: small (~500B), regular (~5KB), big (~100KB), huge (~200KB)")
    parser.add_argument("--compressible", action="store_true",
                        help="Use compressible padding (repeating pattern); default is random bytes")
    parser.add_argument("--batch-size", type=int, default=None,
                        help="Documents per insert_many call (auto-set per template if omitted)")
    args = parser.parse_args()

    if args.user and args.password:
        uri = f"mongodb://{args.user}:{args.password}@{args.host}"
    else:
        uri = f"mongodb://{args.host}"

    tmpl = TEMPLATES[args.template]
    batch_size = args.batch_size if args.batch_size is not None else tmpl["batch"]
    pool, doc_size = build_pool(args.template, args.compressible, batch_size)

    mode = "compressible" if args.compressible else "incompressible (random)"
    print(f"[Config] Template: {args.template} (~{tmpl['size']/1024:.1f} KB target) | Data: {mode}")
    print(f"[Config] {args.ops} ops/sec across {args.collections} collection(s), batch size {batch_size}")
    print(f"[Config] URI: {uri}")

    signal.signal(signal.SIGINT, signal_handler)
    per_coll_ops = args.ops / args.collections
    client = pymongo.MongoClient(uri)

    sharded = is_sharded_cluster(client)
    if sharded:
        print("[Topology] Detected sharded cluster (mongos)")
        enable_sharding(client, DB_NAME)
        for i in range(args.collections):
            coll_name = f"{COLLECTION_PREFIX}_{i}"
            shard_collection(client, DB_NAME, coll_name)
    else:
        print("[Topology] Detected replica set / standalone — skipping sharding")

    start_time = time.time()

    rt = threading.Thread(target=reporter_thread, daemon=True)
    rt.start()

    for i in range(args.collections):
        coll_name = f"{COLLECTION_PREFIX}_{i}"
        t = threading.Thread(target=writer_thread, args=(client, coll_name, per_coll_ops, pool, doc_size, batch_size, sharded))
        t.start()
        threads.append(t)

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        pass

    elapsed = time.time() - start_time
    avg_iops = total_inserted / elapsed if elapsed > 0 else 0
    print(f"\n[Done] Inserted {total_inserted} documents in {elapsed:.2f} seconds.")
    print(f"[Summary] Avg IOPS: {avg_iops:.2f}")


if __name__ == "__main__":
    main()
