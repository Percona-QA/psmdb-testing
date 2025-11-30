import pymongo
import datetime
import random
import time
from bson import ObjectId
from pymongo.collation import Collation

def create_sharded_collection_types(db, create_ts=False, drop_before_creation=False,
                                    create_unique_sharded=False, create_collation_sharded=False):
    collections_metadata = []

    if drop_before_creation:
        db.drop_collection("sharded_range_key_collection")
        db.drop_collection("sharded_hashed_key_collection")
        db.drop_collection("sharded_compound_key_collection")
        db.drop_collection("sharded_unique_key_collection")
        db.drop_collection("sharded_timeseries_collection")
        db.drop_collection("sharded_collation_collection")
        db.drop_collection("sharded_collation_id_shard_collection")
        db.drop_collection("sharded_collation_compound_collection")
        db.drop_collection("sharded_collation_compound_unique_collection")
        db.drop_collection("sharded_compound_hashed_collection")

    db.client.admin.command("enableSharding", db.name)

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

    # Sharded collection with compound hashed shard key {a: 1, b: "hashed", c: 1}
    sharded_compound_hashed = db.sharded_compound_hashed_collection
    compound_hashed_docs = [{"a": i, "b": f"hashed_{i % 10}", "c": i * 2, "name": f"item_{i}", "value": f"value_{i}"}
        for i in range(25)]
    sharded_compound_hashed.insert_many(compound_hashed_docs)
    sharded_compound_hashed.create_index([("a", pymongo.ASCENDING), ("b", pymongo.HASHED), ("c", pymongo.ASCENDING)], name="a_asc_b_hashed_c_asc_shard_key_index")
    db.client.admin.command("shardCollection", f"{db.name}.sharded_compound_hashed_collection", key={"a": 1, "b": "hashed", "c": 1})
    collections_metadata.append({"collection": sharded_compound_hashed, "timeseries": False,
                                "sharded": True, "shard_key": ["a", "b", "c"], "hashed": True})

    # Sharded collection with unique shard key
    if create_unique_sharded:
        sharded_unique_key = db.sharded_unique_key_collection
        db.client.admin.command("shardCollection", f"{db.name}.sharded_unique_key_collection", key={"key_id": 1}, unique=True)
        unique_key_docs = [{"key_id": i, "name": f"unique_item_{i}", "value": f"value_{i}", "status": "active"}
            for i in range(20)]
        sharded_unique_key.insert_many(unique_key_docs)
        collections_metadata.append({"collection": sharded_unique_key, "timeseries": False,
                                    "sharded": True, "shard_key": "key_id", "unique": True})

    # Sharded collections with collation
    if create_collation_sharded:
        fr_collation = Collation(locale='fr', strength=2)

        # Sharded collection with collation and non-_id shard key
        sharded_collation = db.create_collection("sharded_collation_collection", collation=fr_collation)
        sharded_collation.create_index([("key_id", pymongo.ASCENDING)], name="key_id_shard_key_index", collation={"locale": "simple"})
        db.client.admin.command("shardCollection", f"{db.name}.sharded_collation_collection", key={"key_id": 1}, collation={"locale": "simple"})
        collation_docs = [{"key_id": i, "name": f"item_{i}", "value": f"value_{i}", "region": f"region_{i % 3}"}
            for i in range(20)]
        sharded_collation.insert_many(collation_docs)
        collections_metadata.append({"collection": sharded_collation, "timeseries": False,
                                    "sharded": True, "shard_key": "key_id"})

        # Sharded collection with collation and _id as shard key (hashed)
        sharded_collation_id_shard = db.create_collection("sharded_collation_id_shard_collection", collation=fr_collation)
        db.client.admin.command("shardCollection", f"{db.name}.sharded_collation_id_shard_collection", key={"_id": "hashed"}, collation={"locale": "simple"})
        collation_id_shard_docs = [{"_id": ObjectId(), "item_id": f"item_{i}", "category_id": i % 10, "amount": 100.0 + i, "status": "pending"}
            for i in range(30)]
        sharded_collation_id_shard.insert_many(collation_id_shard_docs)
        collections_metadata.append({"collection": sharded_collation_id_shard, "timeseries": False,
                                    "sharded": True, "shard_key": "_id", "hashed": True, "collation_id_shard": True})

        # Sharded collection with collation and compound shard key
        sharded_collation_compound = db.create_collection("sharded_collation_compound_collection", collation=fr_collation)
        sharded_collation_compound.create_index([("category", pymongo.ASCENDING), ("item_id", pymongo.ASCENDING)],
            name="category_item_id_shard_key_index", collation={"locale": "simple"})
        db.client.admin.command("shardCollection", f"{db.name}.sharded_collation_compound_collection",
            key={"category": 1, "item_id": 1}, collation={"locale": "simple"})
        collation_compound_docs = [{"item_id": i, "category": f"cat_{i % 5}", "name": f"item_{i}", "price": 50.0 + i}
            for i in range(25)]
        sharded_collation_compound.insert_many(collation_compound_docs)
        collections_metadata.append({"collection": sharded_collation_compound, "timeseries": False,
                                    "sharded": True, "shard_key": ["category", "item_id"]})

        # Sharded collection with collation, compound and unique shard key
        if create_unique_sharded and create_collation_sharded:
            sharded_collation_compound_unique = db.create_collection("sharded_collation_compound_unique_collection", collation=fr_collation)
            sharded_collation_compound_unique.create_index([("category", pymongo.ASCENDING), ("item_id", pymongo.ASCENDING)],
                name="category_item_id_shard_key_index", unique=True, collation={"locale": "simple"})
            db.client.admin.command("shardCollection", f"{db.name}.sharded_collation_compound_unique_collection",
                key={"category": 1, "item_id": 1}, unique=True, collation={"locale": "simple"})
            collation_compound_unique_docs = [{"item_id": i, "category": f"cat_{i % 5}", "name": f"item_{i}", "price": 50.0 + i}
                for i in range(25)]
            sharded_collation_compound_unique.insert_many(collation_compound_unique_docs)
            collections_metadata.append({"collection": sharded_collation_compound_unique, "timeseries": False,
                                        "sharded": True, "shard_key": ["category", "item_id"], "unique": True})

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

def perform_crud_ops_sharded_collection(collection, shard_key, timeseries=False, hashed=False, no_shard_key=True,
                                        update_shard_key=True, unique=False, collation_id_shard=False):
    # sharded_timeseries_collection
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

        # findOneAnd* operations for timeseries collections
        findoneand_time = base_time + datetime.timedelta(seconds=200)
        collection.find_one_and_update(
            {"sensor_id": sensor_id, "timestamp": findoneand_time},
            {"$set": {"sensor_id": sensor_id, "timestamp": findoneand_time, "temperature": 35.0, "humidity": 75.0, "pressure": 1030.0}},
            upsert=True
        )
        collection.find_one_and_update(
            {"sensor_id": sensor_id, "timestamp": findoneand_time},
            {"$set": {"temperature": 36.0, "humidity": 76.0}}
        )
        collection.find_one_and_replace(
            {"sensor_id": sensor_id, "timestamp": findoneand_time + datetime.timedelta(seconds=1)},
            {"sensor_id": sensor_id, "timestamp": findoneand_time + datetime.timedelta(seconds=1), "temperature": 40.0, "humidity": 80.0, "pressure": 1040.0},
            upsert=True
        )
        collection.find_one_and_delete({"sensor_id": sensor_id, "timestamp": findoneand_time + datetime.timedelta(seconds=1)})

    # sharded_compound_key_collection, sharded_collation_compound_collection, sharded_collation_compound_unique_collection
    # sharded_compound_hashed_collection
    elif isinstance(shard_key, list):
        collection_name = collection.name
        is_compound_hashed = "compound_hashed" in collection_name and len(shard_key) == 3

        if is_compound_hashed:
            # Handle compound hashed shard key collection {a: 1, b: "hashed", c: 1}
            a_val = int(time.time() * 1000000) + random.randint(0, 999999)
            b_val = f"hashed_{random.randint(0, 10)}"
            c_val = a_val * 2
            new_doc = {
                "a": a_val,
                "b": b_val,
                "c": c_val,
                "name": f"item_crud_{random.randint(1000, 9999)}",
                "value": f"value_{a_val}"
            }
            query_key1 = {"a": a_val}
            query_key2 = {"a": a_val, "b": b_val, "c": c_val}
        else:
            # Handle standard compound shard key collections
            category = f"cat_{random.randint(0, 10)}"
            item_id = int(time.time() * 1000000) + random.randint(0, 999999)
            new_doc = {
                "item_id": item_id,
                "category": category,
                "name": f"item_crud_{random.randint(1000, 9999)}",
                "price": round(random.uniform(10.0, 1000.0), 2)
            }
            query_key1 = {"category": category}
            query_key2 = {"category": category, "item_id": item_id}
            a_val = category
            b_val = item_id

        collection.insert_one(new_doc)

        if is_compound_hashed:
            new_docs = [
                {
                    "_id": ObjectId(),
                    "a": a_val + i,
                    "b": f"{b_val.split('_')[0]}_{(int(b_val.split('_')[1]) + i) % 10}",
                    "c": (a_val + i) * 2,
                    "name": f"item_batch_{i}",
                    "value": f"value_{a_val + i}"
                }
                for i in range(1, 6)
            ]
        else:
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

        if is_compound_hashed:
            collection.update_one(
                query_key2,
                {"$set": {"name": "Updated Item Name", "value": "updated_value"}}
            )
        else:
            collection.update_one(
                query_key2,
                {"$set": {"name": "Updated Item Name", "price": 999.99}}
            )

        collection.update_many(
            query_key1,
            {"$set": {"last_updated": datetime.datetime.now(datetime.timezone.utc)}}
        )

        if is_compound_hashed:
            replace_doc = dict(query_key2)
            replace_doc.update({"name": "Replaced Item", "value": "replaced_value"})
        else:
            replace_doc = {"category": category, "item_id": item_id, "name": "Replaced Item", "price": 500.0}
        collection.replace_one(query_key2, replace_doc)

        if is_compound_hashed:
            delete_query = {"a": a_val, "b": new_docs[0]["b"], "c": new_docs[0]["c"]}
        else:
            delete_query = {"category": category, "item_id": item_id + 1}
        collection.delete_one(delete_query)

        if is_compound_hashed:
            collection.delete_many(query_key1)
        else:
            collection.delete_many({"category": category, "price": {"$lt": 50.0}})

        if is_compound_hashed:
            upsert_a = int(time.time() * 1000000) + random.randint(1000000, 9999999)
            upsert_b = f"hashed_{random.randint(0, 10)}"
            upsert_c = upsert_a * 2
            upsert_query = {"a": upsert_a, "b": upsert_b, "c": upsert_c}
            upsert_doc = {"a": upsert_a, "b": upsert_b, "c": upsert_c, "name": "Upserted Item", "value": "upserted_value"}
        else:
            upsert_item_id = int(time.time() * 1000000) + random.randint(1000000, 9999999)
            upsert_query = {"category": category, "item_id": upsert_item_id}
            upsert_doc = {"category": category, "item_id": upsert_item_id, "name": "Upserted Item", "price": 200.0}
        collection.update_one(upsert_query, {"$set": upsert_doc}, upsert=True)

        if is_compound_hashed:
            bulk_update_query = {"a": a_val, "b": new_docs[1]["b"], "c": new_docs[1]["c"]}
            bulk_replace_query = {"a": a_val, "b": new_docs[2]["b"], "c": new_docs[2]["c"]}
            bulk_replace_doc = {"a": a_val, "b": new_docs[2]["b"], "c": new_docs[2]["c"], "name": "Bulk Replaced", "value": "bulk_value"}
            bulk_delete_query = {"a": a_val, "b": new_docs[3]["b"], "c": new_docs[3]["c"]}
        else:
            bulk_update_query = {"category": category, "item_id": item_id + 2}
            bulk_replace_query = {"category": category, "item_id": item_id + 3}
            bulk_replace_doc = {"category": category, "item_id": item_id + 3, "name": "Bulk Replaced", "price": 300.0}
            bulk_delete_query = {"category": category, "item_id": item_id + 4}

        bulk_operations = [
            pymongo.UpdateOne(
                bulk_update_query,
                {"$set": {"status": "active"}},
                upsert=True
            ),
            pymongo.UpdateMany(
                query_key1,
                {"$set": {"bulk_updated": True}}
            ),
            pymongo.ReplaceOne(
                bulk_replace_query,
                bulk_replace_doc,
                upsert=True
            ),
            pymongo.DeleteOne(bulk_delete_query)
        ]
        collection.bulk_write(bulk_operations)

        if is_compound_hashed:
            findoneand_a = int(time.time() * 1000000) + random.randint(2000000, 2999999)
            findoneand_b = f"hashed_{random.randint(0, 10)}"
            findoneand_c = findoneand_a * 2
            findoneand_query = {"a": findoneand_a, "b": findoneand_b, "c": findoneand_c}
            findoneand_doc = {"a": findoneand_a, "b": findoneand_b, "c": findoneand_c, "name": "FindOneAndUpdate Upsert", "value": "findoneand_update_value"}
            findoneand_replace_query = {"a": findoneand_a + 1, "b": findoneand_b, "c": (findoneand_a + 1) * 2}
            findoneand_replace_doc = {"a": findoneand_a + 1, "b": findoneand_b, "c": (findoneand_a + 1) * 2, "name": "FindOneAndReplace Upserted", "value": "findoneand_replace_value"}
        else:
            findoneand_item_id = int(time.time() * 1000000) + random.randint(2000000, 2999999)
            findoneand_query = {"category": category, "item_id": findoneand_item_id}
            findoneand_doc = {"category": category, "item_id": findoneand_item_id, "name": "FindOneAndUpdate Upsert", "price": 250.0}
            findoneand_replace_query = {"category": category, "item_id": findoneand_item_id + 1}
            findoneand_replace_doc = {"category": category, "item_id": findoneand_item_id + 1, "name": "FindOneAndReplace Upserted", "price": 450.0}

        collection.find_one_and_update(
            findoneand_query,
            {"$set": findoneand_doc},
            upsert=True
        )
        if is_compound_hashed:
            collection.find_one_and_update(
                findoneand_query,
                {"$set": {"name": "FindOneAndUpdate Upserted New", "value": "findoneand_update_value_new"}}
            )
        else:
            collection.find_one_and_update(
                findoneand_query,
                {"$set": {"name": "FindOneAndUpdate Upserted New", "price": 350.0}}
            )
        collection.find_one_and_replace(
            findoneand_replace_query,
            findoneand_replace_doc,
            upsert=True
        )
        if is_compound_hashed:
            collection.find_one_and_replace(
                findoneand_replace_query,
                {**findoneand_replace_query, "name": "FindOneAndReplace Upserted New", "value": "findoneand_replace_value_new"}
            )
        else:
            collection.find_one_and_replace(
                findoneand_replace_query,
                {"category": category, "item_id": findoneand_item_id + 1, "name": "FindOneAndReplace Upserted New", "price": 550.0}
            )
        collection.find_one_and_delete(findoneand_replace_query)

    # sharded_hashed_key_collection, sharded_collation_id_shard_collection
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

        findoneand_id = ObjectId()
        collection.find_one_and_update(
            {"_id": findoneand_id},
            {"$set": {"_id": findoneand_id, "item_id": "findoneand_update_upsert", "category_id": 666, "amount": 500.0, "status": "findoneand"}},
            upsert=True
        )
        collection.find_one_and_update(
            {"_id": findoneand_id},
            {"$set": {"status": "findoneand_updated", "amount": 600.0}}
        )
        findoneand_replace_id = ObjectId()
        collection.find_one_and_replace(
            {"_id": findoneand_replace_id},
            {"_id": findoneand_replace_id, "item_id": "findoneand_replace_upsert", "category_id": 555, "amount": 700.0, "status": "findoneand_replace"},
            upsert=True
        )
        collection.find_one_and_replace(
            {"_id": findoneand_replace_id},
            {"_id": findoneand_replace_id, "item_id": "findoneand_replace_upserted_new", "category_id": 444, "amount": 800.0, "status": "findoneand_replace_new"}
        )
        collection.find_one_and_delete({"_id": findoneand_replace_id})
    # sharded_range_key_collection, sharded_unique_key_collection, sharded_collation_collection
    else:
        key_id = int(time.time() * 1000000) + random.randint(0, 999999)
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

        upsert_key_id = int(time.time() * 1000000) + random.randint(1000000, 9999999)
        collection.update_one(
            {"key_id": upsert_key_id},
            {"$set": {"key_id": upsert_key_id, "name": "Upserted Item", "value": "upserted_value", "region": "region_upsert"}},
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

        # findOneAnd* operations for range shard key collections
        findoneand_key_id = int(time.time() * 1000000) + random.randint(3000000, 3999999)
        collection.find_one_and_update(
            {"key_id": findoneand_key_id},
            {"$set": {"key_id": findoneand_key_id, "name": "FindOneAndUpdate Upsert", "value": "findoneand_update_value", "region": "region_findoneand"}},
            upsert=True
        )
        collection.find_one_and_update(
            {"key_id": findoneand_key_id},
            {"$set": {"name": "FindOneAndUpdate Upserted New", "value": "findoneand_update_value_new"}}
        )
        findoneand_replace_key_id = int(time.time() * 1000000) + random.randint(4000000, 4999999)
        collection.find_one_and_replace(
            {"key_id": findoneand_replace_key_id},
            {"key_id": findoneand_replace_key_id, "name": "FindOneAndReplace Upserted", "value": "findoneand_replace_value", "region": "region_findoneand_replace"},
            upsert=True
        )
        collection.find_one_and_replace(
            {"key_id": findoneand_replace_key_id},
            {"key_id": findoneand_replace_key_id, "name": "FindOneAndReplace Upserted New", "value": "findoneand_replace_value_new", "region": "region_findoneand_replace_new"}
        )
        collection.find_one_and_delete({"key_id": findoneand_replace_key_id})

    # Documents without shard key fields
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

        # Test scenario when document contains only one part of compound shard key
        if isinstance(shard_key, list) and not hashed:
            collection_name = collection.name
            if "compound_key" in collection_name and "compound_hashed" not in collection_name:
                duplicate_key_test_doc = {
                    "category": f"cat_{random.randint(0, 10)}",
                    "name": f"duplicate_key_test_{random.randint(10000, 99999)}",
                    "price": round(random.uniform(10.0, 1000.0), 2),
                    "description": "Document with category but missing item_id shard key field"
                }
                collection.insert_one(duplicate_key_test_doc)

                duplicate_key_test_docs = [
                    {
                        "category": f"cat_{random.randint(0, 10)}",
                        "name": f"duplicate_key_batch_{i}",
                        "price": round(random.uniform(10.0, 1000.0), 2),
                        "description": f"Batch document {i} with category but no item_id"
                    }
                    for i in range(3)
                ]
                collection.insert_many(duplicate_key_test_docs)

                collection.update_one(
                    {"category": duplicate_key_test_doc["category"], "name": duplicate_key_test_doc["name"]},
                    {"$set": {"name": "Updated Duplicate Key Test", "price": 999.99, "updated": True}}
                )

                collection.replace_one(
                    {"category": duplicate_key_test_docs[0]["category"], "name": duplicate_key_test_docs[0]["name"]},
                    {"category": duplicate_key_test_docs[0]["category"], "name": "Replaced Duplicate Key Test", "price": 888.88, "replaced": True}
                )

                bulk_duplicate_key_ops = [
                    pymongo.UpdateOne(
                        {"category": duplicate_key_test_docs[1]["category"], "name": duplicate_key_test_docs[1]["name"]},
                        {"$set": {"status": "active"}}
                    ),
                    pymongo.ReplaceOne(
                        {"category": duplicate_key_test_docs[2]["category"], "name": duplicate_key_test_docs[2]["name"]},
                        {"category": duplicate_key_test_docs[2]["category"], "name": "Bulk Replaced Duplicate Key", "price": 777.77}
                    ),
                    pymongo.DeleteOne({"category": duplicate_key_test_doc["category"], "name": duplicate_key_test_doc["name"]})
                ]
                collection.bulk_write(bulk_duplicate_key_ops)

    # Shard key updates
    if update_shard_key and not hashed and not timeseries:
        # sharded_compound_key_collection, sharded_collation_compound_collection, sharded_collation_compound_unique_collection
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
        # sharded_range_key_collection, sharded_unique_key_collection, sharded_collation_collection
        else:
            key_id = int(time.time() * 1000000) + random.randint(0, 999999)
            new_key_id = int(time.time() * 1000000) + random.randint(1000000, 1999999)

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