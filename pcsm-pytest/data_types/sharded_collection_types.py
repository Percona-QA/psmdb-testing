import pymongo
import datetime
import random
from bson import ObjectId
from pymongo.collation import Collation

def create_sharded_collection_types(db, create_ts=False, drop_before_creation=False):
    collections_metadata = []

    if drop_before_creation:
        db.drop_collection("sharded_range_key_collection")
        db.drop_collection("sharded_hashed_key_collection")
        db.drop_collection("sharded_compound_key_collection")
        db.drop_collection("sharded_collation_collection")
        db.drop_collection("sharded_timeseries_collection")

    try:
        db.client.admin.command("enableSharding", db.name)
    except pymongo.errors.OperationFailure:
        pass

    # Sharded collection with range-based shard key
    sharded_range_key = db.sharded_range_key_collection
    db.client.admin.command("shardCollection", f"{db.name}.sharded_range_key_collection", key={"key_id": 1})
    range_key_docs = [{"key_id": i, "name": f"item_{i}", "value": f"value_{i}", "region": f"region_{i % 3}"}
        for i in range(20)]
    sharded_range_key.insert_many(range_key_docs)
    collections_metadata.append({"collection": sharded_range_key, "timeseries": False, "sharded": True,
                                "shard_key": "key_id"})

    # Sharded collection with hashed shard key
    sharded_hashed_key = db.sharded_hashed_key_collection
    db.client.admin.command("shardCollection", f"{db.name}.sharded_hashed_key_collection", key={"_id": "hashed"})
    hashed_key_docs = [{"item_id": f"item_{i}", "category_id": i % 10, "amount": 100.0 + i, "status": "pending"}
        for i in range(30)]
    sharded_hashed_key.insert_many(hashed_key_docs)
    collections_metadata.append({"collection": sharded_hashed_key, "timeseries": False,
                                "sharded": True, "shard_key": "_id", "hashed": True})

    # Sharded collection with compound shard key
    sharded_compound_key = db.sharded_compound_key_collection
    compound_key_docs = [{"item_id": i, "category": f"cat_{i % 5}", "name": f"item_{i}", "price": 50.0 + i}
        for i in range(25)]
    sharded_compound_key.insert_many(compound_key_docs)
    sharded_compound_key.create_index([("category", pymongo.ASCENDING), ("item_id", pymongo.ASCENDING)], name="category_item_id_shard_key_index")
    db.client.admin.command("shardCollection", f"{db.name}.sharded_compound_key_collection", key={"category": 1, "item_id": 1})
    collections_metadata.append({"collection": sharded_compound_key, "timeseries": False,
                                "sharded": True, "shard_key": ["category", "item_id"]})

    # Sharded collection with collation
    fr_collation = Collation(locale='fr', strength=2)
    sharded_collation = db.create_collection("sharded_collation_collection", collation=fr_collation)
    fr_strings = ["cote", "coté", "côte", "côté"]
    collation_docs = [{"text_key": idx, "string": string, "value": idx}
        for string_idx, string in enumerate(fr_strings)
        for idx in range(string_idx * 10, string_idx * 10 + 10)]
    sharded_collation.insert_many(collation_docs)
    sharded_collation.create_index([("text_key", pymongo.ASCENDING)], name="text_key_shard_key_index", collation={"locale": "simple"})
    db.client.admin.command("shardCollection", f"{db.name}.sharded_collation_collection", key={"text_key": 1}, collation={"locale": "simple"})
    collections_metadata.append({"collection": sharded_collation, "timeseries": False,
                                "sharded": True, "shard_key": "text_key"})

    # Sharded timeseries collection
    if create_ts:
        db.client.admin.command("shardCollection", f"{db.name}.sharded_timeseries_collection", key={"sensor_id": 1, "timestamp": 1},
            timeseries={
                "timeField": "timestamp",
                "metaField": "sensor_id",
                "granularity": "seconds"})
        sharded_timeseries = db.sharded_timeseries_collection
        base_time = datetime.datetime.now(datetime.timezone.utc)
        timeseries_docs = []
        for sensor_id in range(5):
            for i in range(10):
                timeseries_docs.append({
                    "sensor_id": f"sensor_{sensor_id}",
                    "timestamp": base_time + datetime.timedelta(seconds=i),
                    "temperature": 20.0 + random.uniform(-5, 5),
                    "humidity": 50.0 + random.uniform(-10, 10),
                    "pressure": 1013.25 + random.uniform(-10, 10)
                })
        sharded_timeseries.insert_many(timeseries_docs)
        collections_metadata.append({"collection": sharded_timeseries,"timeseries": True,
                                    "sharded": True, "shard_key": ["sensor_id", "timestamp"]})

    return collections_metadata

def perform_crud_ops_sharded_collection(collection, shard_key, hashed=False, no_shard_key=True, update_shard_key=True, timeseries=False):
    if timeseries:
        sensor_id = f"sensor_{random.randint(0, 10)}"
        base_time = datetime.datetime.now(datetime.timezone.utc)
        new_doc = {
            "sensor_id": sensor_id,
            "timestamp": base_time,
            "temperature": round(20.0 + random.uniform(-5, 5), 2),
            "humidity": round(50.0 + random.uniform(-10, 10), 2),
            "pressure": round(1013.25 + random.uniform(-10, 10), 2)}
        collection.insert_one(new_doc)
        new_docs = [
            {
                "sensor_id": sensor_id,
                "timestamp": base_time + datetime.timedelta(seconds=i),
                "temperature": round(20.0 + random.uniform(-5, 5), 2),
                "humidity": round(50.0 + random.uniform(-10, 10), 2),
                "pressure": round(1013.25 + random.uniform(-10, 10), 2)
            }
            for i in range(1, 6)]
        collection.insert_many(new_docs)
        collection.update_one(
            {"sensor_id": sensor_id, "timestamp": base_time},
            {"$set": {"temperature": 25.0, "humidity": 60.0}})
        collection.update_many(
            {"sensor_id": sensor_id},
            {"$set": {"last_updated": datetime.datetime.now(datetime.timezone.utc)}})
        collection.delete_one({"sensor_id": sensor_id, "timestamp": base_time + datetime.timedelta(seconds=1)})
        collection.delete_many({"sensor_id": sensor_id, "temperature": {"$lt": 15.0}})
        upsert_time = base_time + datetime.timedelta(seconds=100)
        collection.update_one(
            {"sensor_id": sensor_id, "timestamp": upsert_time},
            {"$set": {
                "sensor_id": sensor_id,
                "timestamp": upsert_time,
                "temperature": 30.0,
                "humidity": 70.0,
                "pressure": 1020.0
            }},
            upsert=True)
        bulk_operations = [
            pymongo.UpdateOne(
                {"sensor_id": sensor_id, "timestamp": base_time + datetime.timedelta(seconds=2)},
                {"$set": {"status": "active"}},
                upsert=True),
            pymongo.UpdateMany(
                {"sensor_id": sensor_id},
                {"$set": {"bulk_updated": True}}),
            pymongo.DeleteOne({"sensor_id": sensor_id, "timestamp": base_time + datetime.timedelta(seconds=3)})]
        collection.bulk_write(bulk_operations)

    elif isinstance(shard_key, list):
        category = f"cat_{random.randint(0, 10)}"
        item_id = random.randint(100, 999)
        new_doc = {
            "item_id": item_id,
            "category": category,
            "name": f"item_crud_{random.randint(1000, 9999)}",
            "price": round(random.uniform(10.0, 1000.0), 2)
        }
        collection.insert_one(new_doc)

        new_docs = [
            {
                "_id": ObjectId(),
                "item_id": item_id + i,
                "category": category,
                "name": f"item_batch_{i}",
                "price": round(random.uniform(10.0, 1000.0), 2)
            }
            for i in range(1, 6)
        ]
        collection.insert_many(new_docs)

        collection.update_one(
            {"category": category, "item_id": item_id},
            {"$set": {"name": "Updated Item Name", "price": 999.99}}
        )

        collection.update_many(
            {"category": category},
            {"$set": {"last_updated": datetime.datetime.now(datetime.timezone.utc)}}
        )

        collection.replace_one(
            {"category": category, "item_id": item_id},
            {"category": category, "item_id": item_id, "name": "Replaced Item", "price": 500.0}
        )

        collection.delete_one({"category": category, "item_id": item_id + 1})
        collection.delete_many({"category": category, "price": {"$lt": 50.0}})

        collection.update_one(
            {"category": category, "item_id": 9999},
            {"$set": {"category": category, "item_id": 9999, "name": "Upserted Item", "price": 200.0}},
            upsert=True
        )

        bulk_operations = [
            pymongo.UpdateOne(
                {"category": category, "item_id": item_id + 2},
                {"$set": {"status": "active"}},
                upsert=True
            ),
            pymongo.UpdateMany(
                {"category": category},
                {"$set": {"bulk_updated": True}}
            ),
            pymongo.ReplaceOne(
                {"category": category, "item_id": item_id + 3},
                {"category": category, "item_id": item_id + 3, "name": "Bulk Replaced", "price": 300.0},
                upsert=True
            ),
            pymongo.DeleteOne({"category": category, "item_id": item_id + 4})
        ]
        collection.bulk_write(bulk_operations)

    elif hashed:
        new_doc = {
            "_id": ObjectId(),
            "item_id": f"item_crud_{random.randint(10000, 99999)}",
            "category_id": random.randint(1, 100),
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "status": "pending"
        }
        collection.insert_one(new_doc)

        new_docs = [
            {
                "_id": ObjectId(),
                "item_id": f"item_batch_{i}",
                "category_id": random.randint(1, 100),
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "status": "pending"
            }
            for i in range(5)
        ]
        collection.insert_many(new_docs)

        collection.update_one(
            {"item_id": new_doc["item_id"]},
            {"$set": {"status": "completed", "amount": 1500.0}}
        )
        collection.update_many(
            {"status": "pending"},
            {"$set": {"last_updated": datetime.datetime.now(datetime.timezone.utc)}}
        )

        collection.replace_one(
            {"item_id": new_doc["item_id"]},
            {"_id": new_doc["_id"], "item_id": "replaced_item", "category_id": 999, "amount": 2000.0, "status": "replaced"}
        )

        collection.delete_one({"item_id": new_docs[0]["item_id"]})
        collection.delete_many({"amount": {"$lt": 50.0}})

        upsert_id = ObjectId()
        collection.update_one(
            {"_id": upsert_id},
            {"$set": {"_id": upsert_id, "item_id": "upserted_item", "category_id": 888, "amount": 300.0, "status": "upserted"}},
            upsert=True
        )

        bulk_operations = [
            pymongo.UpdateOne(
                {"item_id": new_docs[1]["item_id"]},
                {"$set": {"status": "processing"}},
                upsert=True
            ),
            pymongo.UpdateMany(
                {"status": "pending"},
                {"$set": {"bulk_processed": True}}
            ),
            pymongo.ReplaceOne(
                {"item_id": new_docs[2]["item_id"]},
                {"_id": new_docs[2]["_id"], "item_id": "bulk_replaced", "category_id": 777, "amount": 400.0, "status": "bulk"},
                upsert=True
            ),
            pymongo.DeleteOne({"item_id": new_docs[3]["item_id"]})
        ]
        collection.bulk_write(bulk_operations)
    else:
        if shard_key == "text_key":
            key_id = random.randint(100, 999)
            new_doc = {
                "_id": ObjectId(),
                "text_key": key_id,
                "string": "cote",
                "value": key_id
            }
            collection.insert_one(new_doc)

            new_docs = [
                {
                    "_id": ObjectId(),
                    "text_key": key_id + i,
                    "string": ["cote", "coté", "côte", "côté"][i % 4],
                    "value": key_id + i
                }
                for i in range(1, 6)
            ]
            collection.insert_many(new_docs)

            collection.update_one(
                {"text_key": key_id},
                {"$set": {"string": "updated", "value": 9999}}
            )
            collection.update_many(
                {"string": "cote"},
                {"$set": {"last_updated": datetime.datetime.now(datetime.timezone.utc)}}
            )

            collection.replace_one(
                {"text_key": key_id},
                {"text_key": key_id, "string": "replaced", "value": 5000}
            )

            collection.delete_one({"text_key": key_id + 1})
            collection.delete_many({"value": {"$lt": key_id + 10}})

            collection.update_one(
                {"text_key": 9999},
                {"$set": {"text_key": 9999, "string": "upserted", "value": 2000}},
                upsert=True
            )

            bulk_operations = [
                pymongo.UpdateOne(
                    {"text_key": key_id + 2},
                    {"$set": {"status": "active"}},
                    upsert=True
                ),
                pymongo.UpdateMany(
                    {"string": "cote"},
                    {"$set": {"bulk_updated": True}}
                ),
                pymongo.ReplaceOne(
                    {"text_key": key_id + 3},
                    {"text_key": key_id + 3, "string": "bulk_replaced", "value": 3000},
                    upsert=True
                ),
                pymongo.DeleteOne({"text_key": key_id + 4})
            ]
            collection.bulk_write(bulk_operations)
        else:
            key_id = random.randint(100, 999)
            new_doc = {
                "_id": ObjectId(),
                "key_id": key_id,
                "name": f"item_crud_{random.randint(1000, 9999)}",
                "value": f"value_{key_id}",
                "region": f"region_{random.randint(0, 5)}"
            }
            collection.insert_one(new_doc)
            new_docs = [
                {
                    "_id": ObjectId(),
                    "key_id": key_id + i,
                    "name": f"item_batch_{i}",
                    "value": f"value_{key_id + i}",
                    "region": f"region_{i % 3}"
                }
                for i in range(1, 6)
            ]
            collection.insert_many(new_docs)
            collection.update_one(
                {"key_id": key_id},
                {"$set": {"name": "Updated Item", "value": "updated_value"}}
            )
            collection.update_many(
                {"region": new_doc["region"]},
                {"$set": {"last_updated": datetime.datetime.now(datetime.timezone.utc)}}
            )

            collection.replace_one(
                {"key_id": key_id},
                {"key_id": key_id, "name": "Replaced Item", "value": "replaced_value", "region": "region_new"}
            )

            collection.delete_one({"key_id": key_id + 1})
            collection.delete_many({"region": new_doc["region"], "key_id": {"$lt": key_id + 10}})

            collection.update_one(
                {"key_id": 9999},
                {"$set": {"key_id": 9999, "name": "Upserted Item", "value": "upserted_value", "region": "region_upsert"}},
                upsert=True
            )

            bulk_operations = [
                pymongo.UpdateOne(
                    {"key_id": key_id + 2},
                    {"$set": {"status": "active"}},
                    upsert=True
                ),
                pymongo.UpdateMany(
                    {"region": new_doc["region"]},
                    {"$set": {"bulk_updated": True}}
                ),
                pymongo.ReplaceOne(
                    {"key_id": key_id + 3},
                    {"key_id": key_id + 3, "name": "Bulk Replaced", "value": "bulk_value", "region": "region_bulk"},
                    upsert=True
                ),
                pymongo.DeleteOne({"key_id": key_id + 4})
            ]
            collection.bulk_write(bulk_operations)

    if no_shard_key and not timeseries:
        no_shard_key_doc = {
            "name": f"item_no_shard_{random.randint(10000, 99999)}",
            "value": f"value_no_shard_{random.randint(1000, 9999)}",
            "description": "Document without shard key fields"
        }
        collection.insert_one(no_shard_key_doc)

        no_shard_key_docs = [
            {
                "name": f"item_no_shard_batch_{i}",
                "value": f"value_no_shard_{i}",
                "description": f"Batch document {i} without shard key"
            }
            for i in range(3)
        ]
        collection.insert_many(no_shard_key_docs)

        collection.update_one(
            {"name": no_shard_key_doc["name"]},
            {"$set": {"name": "Updated Without Shard Key", "value": "updated_value", "updated": True}}
        )

        collection.update_many(
            {"description": {"$exists": True}},
            {"$set": {"last_updated": datetime.datetime.now(datetime.timezone.utc)}}
        )

        collection.replace_one(
            {"name": no_shard_key_docs[0]["name"]},
            {"name": "Replaced Without Shard Key", "value": "replaced_value", "replaced": True}
        )

        collection.delete_one({"name": no_shard_key_docs[1]["name"]})
        collection.delete_many({"description": {"$exists": True}, "value": {"$regex": "^value_no_shard"}})

        collection.update_one(
            {"name": "Upserted Without Shard Key"},
            {"$set": {"name": "Upserted Without Shard Key", "value": "upserted_value", "upserted": True}},
            upsert=True
        )

        bulk_operations_no_shard = [
            pymongo.UpdateOne(
                {"name": no_shard_key_docs[2]["name"]},
                {"$set": {"status": "active"}},
                upsert=True
            ),
            pymongo.UpdateMany(
                {"description": {"$exists": True}},
                {"$set": {"bulk_updated": True}}
            ),
            pymongo.ReplaceOne(
                {"name": "Bulk Replace No Shard"},
                {"name": "Bulk Replaced Without Shard Key", "value": "bulk_value"},
                upsert=True
            ),
            pymongo.DeleteOne({"name": "Replaced Without Shard Key"})
        ]
        collection.bulk_write(bulk_operations_no_shard)

    if update_shard_key and not hashed and not timeseries:
        if isinstance(shard_key, list):
            category = f"cat_{random.randint(0, 10)}"
            item_id = random.randint(100, 999)
            new_category = f"cat_{random.randint(0, 10)}"
            new_item_id = random.randint(1000, 9999)

            shard_key_update_doc = {
                "category": category,
                "item_id": item_id,
                "name": f"item_shard_key_update_{random.randint(10000, 99999)}",
                "price": round(random.uniform(10.0, 1000.0), 2)
            }
            collection.insert_one(shard_key_update_doc)

            collection.update_one(
                {"category": category, "item_id": item_id},
                {"$set": {"category": new_category, "item_id": new_item_id, "price": 1500.0, "shard_key_updated": True}}
            )

            collection.replace_one(
                {"category": new_category, "item_id": new_item_id},
                {"category": new_category, "item_id": new_item_id, "name": "Replaced With New Shard Key", "price": 2000.0}
            )
        elif shard_key == "text_key":
            key_id = random.randint(100, 999)
            new_key_id = random.randint(2000, 9999)

            shard_key_update_doc = {
                "_id": ObjectId(),
                "text_key": key_id,
                "string": "shard_key_update",
                "value": key_id
            }
            collection.insert_one(shard_key_update_doc)

            collection.update_one(
                {"text_key": key_id},
                {"$set": {"text_key": new_key_id, "value": new_key_id, "shard_key_updated": True}}
            )

            collection.replace_one(
                {"text_key": new_key_id},
                {"text_key": new_key_id, "string": "Replaced With New Shard Key", "value": new_key_id}
            )
        else:
            key_id = random.randint(100, 999)
            new_key_id = random.randint(2000, 9999)

            shard_key_update_doc = {
                "_id": ObjectId(),
                "key_id": key_id,
                "name": f"item_shard_key_update_{random.randint(10000, 99999)}",
                "value": f"value_{key_id}",
                "region": f"region_{random.randint(0, 5)}"
            }
            collection.insert_one(shard_key_update_doc)

            collection.update_one(
                {"key_id": key_id},
                {"$set": {"key_id": new_key_id, "value": f"value_{new_key_id}", "shard_key_updated": True}}
            )

            collection.replace_one(
                {"key_id": new_key_id},
                {"key_id": new_key_id, "name": "Replaced With New Shard Key", "value": f"value_{new_key_id}", "region": "region_updated"}
            )