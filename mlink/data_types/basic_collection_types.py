import pymongo
import datetime
import uuid
import re
from bson import Decimal128, ObjectId, Binary, Code, Timestamp, Int64, DBRef, UUID_SUBTYPE
from pymongo.collation import Collation

def create_collection_types(db, create_ts=False, drop_before_creation=False):
    collections_metadata = []

    if drop_before_creation:
        db.drop_collection("regular_collection")
        db.drop_collection("fr_collation_collection")
        db.drop_collection("capped_logs")
        db.drop_collection("timeseries_data")

    # Regular Collection
    regular_collection = db.regular_collection
    bson_docs = [
        {
            "_id": ObjectId(),
            "string": "Hello, World!",
            "int": 42,
            "long": Int64(1234567890123456789),
            "double": 3.14159,
            "decimal": Decimal128("1234567890.123456789"),
            "boolean": False,
            "date": datetime.datetime.now(datetime.timezone.utc),
            "array": [1, "two", 3.14, True, None],
            "object": {"nested_key": "nested_value"},
            "binary": Binary(b"\x00\x01\x02\x03"),
            "null": None,
            "regex": re.compile("^regex$", re.IGNORECASE),
            "javascript": Code("function() { return 42; }"),
            "timestamp": Timestamp(int(datetime.datetime.now(datetime.timezone.utc).timestamp()), 1),
            "dbref": DBRef("other_collection", ObjectId()),
            "uuid": Binary.from_uuid(uuid.uuid4(), UUID_SUBTYPE)
        }
        for _ in range(5)
    ]
    regular_collection.insert_many(bson_docs)
    collections_metadata.append({"collection": regular_collection, "capped": False, "timeseries": False})

    # Collection with fr collation
    fr_collation = Collation(locale='fr', strength=2)
    fr_collation_collection = db.create_collection("fr_collation_collection", collation=fr_collation)
    french_docs = [
        {"string": "cote"},
        {"string": "coté"},
        {"string": "côte"},
        {"string": "côté"}
    ]
    fr_collation_collection.insert_many(french_docs)
    collections_metadata.append({"collection": fr_collation_collection, "capped": False, "timeseries": False})

    # Capped Collection
    db.create_collection("capped_logs", capped=True, size=1024 * 1024, max=20)
    capped_collection = db.capped_logs
    capped_collection.insert_many([
        {"timestamp": datetime.datetime.now(datetime.timezone.utc), "log": "Test log 1"},
        {"timestamp": datetime.datetime.now(datetime.timezone.utc), "log": "Test log 2"}
    ])
    collections_metadata.append({"collection": capped_collection, "capped": True, "timeseries": False})

    if create_ts:
        # Time-series Collection
        db.create_collection("timeseries_data", timeseries={"timeField": "timestamp", "metaField": "metadata", "granularity": "seconds"})
        timeseries_collection = db.timeseries_data
        ts_docs = [
            {
                "timestamp": datetime.datetime.now(datetime.timezone.utc),
                "metadata": {"sensor": f"sensor_{_}"},
                "value": 20.5 + _
            }
            for _ in range(50)
        ]
        timeseries_collection.insert_many(ts_docs)
        collections_metadata.append({"collection": timeseries_collection, "capped": False, "timeseries": True})

    return collections_metadata

def perform_crud_ops_collection(collection, capped=False, timeseries=False):
    #https://www.mongodb.com/docs/manual/core/timeseries/timeseries-limitations/#updates
    if timeseries:

        new_docs = [
            {"timestamp": datetime.datetime.now(datetime.timezone.utc), "metadata": {"sensor": "pressure"}, "value": 101.3},
            {"timestamp": datetime.datetime.now(datetime.timezone.utc), "metadata": {"sensor": "wind"}, "value": 5.2}
        ]
        collection.insert_many(new_docs)
        collection.update_many(
            {"metadata.sensor": {"$exists": True}},
            {"$set": {"metadata.sensor_group": "environment"}},
        )
        return

    new_doc = {
        "_id": ObjectId(),
        "string": "New Inserted String",
        "int": 99,
        "boolean": False
    }
    collection.insert_one(new_doc)

    new_docs = [
        {"string": "Batch Insert 1", "int": 100},
        {"string": "Batch Insert 2", "int": 200},
        {"string": "Batch Insert 3", "int": 10},
        {"string": "Batch Insert 4", "int": 5},
        {"string": "Batch Insert 5", "boolean": False},
        {"string": "Batch Insert 6", "boolean": True}
    ]
    collection.insert_many(new_docs)

    collection.update_one({"string": "New Inserted String"}, {"$set": {"string": "Updated String"}})
    collection.update_many({}, {"$set": {"boolean": True}})

    collection.replace_one(
        {"string": "Updated String"},
        {"string": "Completely Replaced", "int": 5}
    )

    collection.delete_one({"string": "Batch Insert 3"})
    collection.delete_many({"int": {"$gt": 50}})

    collection.update_one(
        {"string": "Upserted Document"},
        {"$set": {"string": "Upserted Document", "int": 150}},
        upsert=True
    )

    collection.update_one(
        {"string": "Upserted Document"},
        {"$set": {"string": "Upserted Document New", "int": 5}},
        upsert=True
    )

    collection.update_many(
        {"category": "missing"},
        {"$set": {"category": "default"}},
        upsert=True
    )

    collection.find_one_and_update(
        {"string": "FindOneAndUpdate Upsert"},
        {"$set": {"string": "FindOneAndUpdate Upserted"}},
        upsert=True
    )

    collection.find_one_and_update(
        {"string": "FindOneAndUpdate Upserted"},
        {"$set": {"string": "FindOneAndUpdate Upserted New"}},
        upsert=True
    )

    collection.find_one_and_replace(
        {"string": "FindOneAndReplace Upsert"},
        {"string": "FindOneAndReplace Upserted"},
        upsert=True
    )

    collection.find_one_and_replace(
        {"string": "FindOneAndReplace Upserted"},
        {"string": "FindOneAndReplace Upserted New"},
        upsert=True
    )

    collection.find_one_and_delete({"string": "Batch Insert 6"})

    # https://www.mongodb.com/docs/manual/reference/method/db.collection.bulkWrite/#capped-collections
    bulk_operations = [
        pymongo.UpdateOne({"string": "Test"}, {"$set": {"status": "processed"}}, upsert=True),
        pymongo.UpdateOne({"string": "FindOneAndUpdate Upserted New"}, {"$set": {"status": "processed"}}, upsert=True),
        pymongo.UpdateMany({"category": "default"}, {"$set": {"processed": True}}, upsert=True),
        pymongo.UpdateMany({"category": "missing"}, {"$set": {"added": True}}, upsert=True),
        pymongo.ReplaceOne({"string": "Test"}, {"string": "Test Upserted Bulk"}, upsert=True),
        pymongo.ReplaceOne({"string": "FindOneAndReplace Upserted New"}, {"string": "FindOneAndReplace Upserted Bulk"}, upsert=True)
    ]

    if not capped:
        bulk_operations += [
            pymongo.DeleteOne({"string": "Batch Insert 1"}),
            pymongo.DeleteMany({"int": {"$lt": 50}})
        ]

    collection.bulk_write(bulk_operations)

