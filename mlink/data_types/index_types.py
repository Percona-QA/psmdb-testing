import pymongo
import datetime
import uuid
from bson import Decimal128, ObjectId, Binary, Code, Timestamp, Int64, DBRef, UUID_SUBTYPE
from gridfs import GridFS

def create_index_types(db, drop_before_creation=False):
    collections = [
        "geo_indexes", "numeric_indexes", "hashed_indexes", "ttl_indexes",
        "sparse_indexes", "partial_indexes", "text_indexes", "regular_text_indexes",
        "wildcard_text_indexes", "wildcard_indexes", "multi_key_indexes",
        "clustered_collection", "unique_compound", "collation_collection"
    ]

    if drop_before_creation:
        for collection in collections:
            db[collection].drop_indexes()
            db.drop_collection(collection)

    # Geospatial Indexes (2D, 2D Sphere)
    geo_collection = db.geo_indexes
    geo_collection.insert_many([
        {"location_2d": [-122.4194, 37.7749]},
        {"location_2dsphere": {"type": "Point", "coordinates": [-74.0060, 40.7128]}},
    ])
    geo_collection.create_index([("location_2d", pymongo.GEO2D)], name="2d_index")
    geo_collection.create_index([("location_2dsphere", pymongo.GEOSPHERE)], name="2dsphere_index")

    # Numeric Indexes (Sparse vs Non-Sparse)
    numeric_collection = db.numeric_indexes
    numeric_collection.insert_many([
        {"int_field": i, "double_field": i * 1.5} for i in range(10)
    ])
    numeric_collection.create_index([("int_field", pymongo.ASCENDING)], name="single_field_index")
    numeric_collection.create_index([("int_field", pymongo.ASCENDING), ("double_field", pymongo.DESCENDING)], name="compound_index")
    numeric_collection.create_index([("double_field", pymongo.ASCENDING)], name="sparse_numeric_index", sparse=True)

    # Hashed Indexes (Single-Field vs Compound)
    hashed_collection = db.hashed_indexes
    hashed_collection.insert_many([
        {"hashed_field": f"user_{i}", "secondary_field": f"extra_{i}"} for i in range(10)
    ])
    hashed_collection.create_index([("hashed_field", pymongo.HASHED)], name="hashed_index")
    hashed_collection.create_index([("hashed_field", pymongo.HASHED), ("secondary_field", pymongo.ASCENDING)], name="compound_hashed_index")

    # TTL Index
    ttl_collection = db.ttl_indexes
    ttl_collection.insert_many([
        {"created_at": datetime.datetime.now(datetime.timezone.utc), "short_lived": True},
        {"created_at": datetime.datetime.now(datetime.timezone.utc), "long_lived": True}
    ])
    ttl_collection.create_index([("created_at", pymongo.ASCENDING)], name="short_ttl_index", expireAfterSeconds=3600)

    # Sparse Index
    sparse_collection = db.sparse_indexes
    sparse_collection.insert_many([
        {"sparse_field": "exists"},
        {"another_field": "this does not have sparse_field"}
    ])
    sparse_collection.create_index([("sparse_field", pymongo.ASCENDING)], name="sparse_index", sparse=True)

    # Partial Index
    partial_collection = db.partial_indexes
    partial_collection.insert_many([
        {"partial_field": "indexed"},
        {"non_partial_field": "not indexed"}
    ])
    partial_collection.create_index(
        [("partial_field", pymongo.ASCENDING)],
        name="partial_index",
        partialFilterExpression={"partial_field": {"$exists": True}}
    )

    # Regular Text Index
    text_collection = db.text_indexes
    text_collection.insert_many([
        {"content": "Hello MongoDB", "extra": "Some extra data"},
        {"content": "Pytest integration testing", "extra": "Another document"}
    ])
    text_collection.create_index([("content", pymongo.TEXT)], name="regular_text_index")

    # Regular Text Index with Weights
    regular_text_collection = db.regular_text_indexes
    regular_text_collection.insert_many([
        {"title": "MongoDB Basics", "description": "A guide to MongoDB indexes"},
        {"title": "Advanced MongoDB", "description": "Deep dive into text search"}
    ])
    regular_text_collection.create_index(
        [("title", pymongo.TEXT), ("description", pymongo.TEXT)],
        name="regular_text_index_with_weights",
        weights={"title": 5, "description": 1}
    )

    # Wildcard Text Index
    wildcard_text_collection = db.wildcard_text_indexes
    wildcard_text_collection.insert_many([
        {"content": "MongoDB wildcard indexing", "extra": "Example document"},
        {"random_field": "This should also be searchable"},
        {"nested": {"field": "Wildcard indexing applies here too"}}
    ])
    wildcard_text_collection.create_index(
        [("$**", pymongo.TEXT)],
        name="wildcard_text_index"
    )

    # Wildcard Index Variations
    wildcard_collection = db.wildcard_indexes
    wildcard_collection.insert_many([
        {"field1": "value1", "field2": "value2", "nested": {"subfield": "nested_value"}},
        {"field1": "another_value", "extra_field": "extra_data"}
    ])
    wildcard_collection.create_index([("$**", pymongo.ASCENDING)], name="wildcard_index")
    wildcard_collection.create_index(
        [("$**", pymongo.ASCENDING)],
        name="filtered_wildcard_index",
        partialFilterExpression={"extra_field": {"$exists": True}}
    )

    # Multi-key Index
    multi_key_collection = db.multi_key_indexes
    multi_key_collection.insert_many([
        {"tags": ["mongodb", "database", "index"]},
        {"tags": ["pytest", "testing"]},
        {"tags": ["performance", "optimization"]}
    ])
    multi_key_collection.create_index([("tags", pymongo.ASCENDING)], name="multi_key_index")

    # Clustered Index (MongoDB 5.3+)
    db.create_collection(
        "clustered_collection",
        clusteredIndex={"key": {"_id": 1}, "unique": True}
    )
    clustered_collection = db.clustered_collection
    clustered_collection.insert_many([
        {"_id": 1, "name": "Alice"},
        {"_id": 2, "name": "Bob"}
    ])

    # Unique Compound Index
    unique_compound_collection = db.unique_compound
    unique_compound_collection.insert_many([
        {"email": "user1@example.com", "phone": "1234567890"},
        {"email": "user2@example.com", "phone": "0987654321"}
    ])
    unique_compound_collection.create_index(
        [("email", pymongo.ASCENDING), ("phone", pymongo.ASCENDING)],
        name="unique_email_phone_index",
        unique=True
    )

    # Collation Index (Case-Insensitive Sorting)
    collation_collection = db.collation_collection
    collation_collection.insert_many([
        {"name": "Alice"},
        {"name": "bob"},
        {"name": "Charlie"},
        {"name": "alice"},
        {"name": "BOB"}
    ])
    collation_collection.create_index(
        [("name", pymongo.ASCENDING)],
        name="collation_index",
        collation=pymongo.collation.Collation(locale="en", strength=2)
    )
