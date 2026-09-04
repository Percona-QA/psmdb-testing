import argparse
import os
import time
from datetime import datetime, timezone

import pymongo

# Continuously inserts documents into the source cluster so PCSM's live
# replication metrics (eventsRead/eventsApplied - the "Events Rate" dashboard
# panel) have something ongoing to show, instead of the single one-off insert
# the main data-generation step does. Meant to be launched in the background
# (see start_background_writer() in test_plm.py) and killed once the test no
# longer needs live traffic - it loops forever on its own otherwise.

def parse_args():
    parser = argparse.ArgumentParser(description="Continuously write documents to MongoDB")
    parser.add_argument("--port", type=int, default=27017, help="MongoDB port to connect to")
    parser.add_argument("--dbname", default=os.getenv("DBNAME", "test_db"))
    parser.add_argument("--collection", default="live_writes")
    parser.add_argument("--interval", type=float, default=float(os.getenv("WRITE_INTERVAL", "0.5")),
                         help="Seconds to sleep between inserts")
    return parser.parse_args()

def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def main():
    args = parse_args()
    client = pymongo.MongoClient(f"mongodb://127.0.0.1:{args.port}/")
    coll = client[args.dbname][args.collection]
    log(f"Starting background writer: db={args.dbname} collection={args.collection} "
        f"port={args.port} interval={args.interval}s")

    counter = 0
    while True:
        try:
            coll.insert_one({"ts": datetime.now(timezone.utc), "counter": counter})
            counter += 1
        except Exception as e:
            log(f"Insert failed (will keep retrying): {e}")
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
