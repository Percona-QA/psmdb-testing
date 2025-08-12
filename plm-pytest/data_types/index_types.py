import pymongo
import datetime


def create_index_types(db, drop_before_creation=False):
    collections = [
        "geo_indexes",
        "hashed_indexes",
        "ttl_indexes",
        "partial_indexes",
        "text_indexes",
        "regular_text_indexes",
        "wildcard_text_indexes",
        "wildcard_indexes",
        "multi_key_indexes",
        "clustered_collection",
        "compound_indexes",
        "hidden_indexes",
    ]

    if drop_before_creation:
        for collection in collections:
            db[collection].drop_indexes()
            db.drop_collection(collection)

    # Geospatial Index Variants
    geo_collection = db.geo_indexes
    geo_collection.insert_many(
        [
            {"location_2d": [-122.4194, 37.7749]},
            {"location_2dsphere": {"type": "Point", "coordinates": [-74.0060, 40.7128]}},
        ]
    )

    geo_collection.create_index([("location_2d", pymongo.GEO2D)], name="2d_index", min=-180, max=180, bits=32)
    geo_collection.create_index([("location_2dsphere", pymongo.GEOSPHERE)], name="2dsphere_index")

    # Hashed Index Variants
    hashed_collection = db.hashed_indexes
    hashed_collection.insert_many([{"hashed_field": f"user_{i}", "secondary_field": f"extra_{i}"} for i in range(10)])
    hashed_collection.create_index([("hashed_field", pymongo.HASHED)], name="hashed_basic_index")
    hashed_collection.create_index(
        [("hashed_field", pymongo.HASHED)],
        name="hashed_partial_index",
        partialFilterExpression={"secondary_field": {"$exists": True}},
    )
    hashed_collection.create_index([("hashed_field", pymongo.HASHED)], name="hashed_sparse_index", sparse=True)
    hashed_collection.create_index(
        [("hashed_field", pymongo.HASHED), ("secondary_field", pymongo.ASCENDING)], name="hashed_compound_index"
    )

    # TTL Index Variants
    ttl_collection = db.ttl_indexes
    ttl_collection.insert_many(
        [
            {"created_at": datetime.datetime.now(datetime.timezone.utc), "short_lived": True},
            {"created_at": datetime.datetime.now(datetime.timezone.utc), "long_lived": True},
        ]
    )
    ttl_collection.create_index([("created_at", pymongo.ASCENDING)], name="ttl_index", expireAfterSeconds=3600)
    ttl_collection.create_index(
        [("created_at", pymongo.ASCENDING)],
        name="ttl_partial_index",
        expireAfterSeconds=7200,
        partialFilterExpression={"short_lived": True},
    )

    # Partial Index
    partial_collection = db.partial_indexes
    partial_collection.insert_many([{"partial_field": "indexed"}, {"non_partial_field": "not indexed"}])
    partial_collection.create_index(
        [("partial_field", pymongo.ASCENDING)],
        name="partial_index",
        partialFilterExpression={"partial_field": {"$exists": True}},
    )

    # Regular Text Index
    text_collection = db.text_indexes
    text_collection.insert_many(
        [
            {"content": "Hello MongoDB", "extra": "Some extra data"},
            {"content": "Pytest integration testing", "extra": "Another document"},
        ]
    )
    text_collection.create_index([("content", pymongo.TEXT)], name="regular_text_index", unique=True)

    # Regular Text Index with Weights
    regular_text_collection = db.regular_text_indexes
    regular_text_collection.insert_many(
        [
            {"title": "MongoDB Basics", "description": "A guide to MongoDB indexes"},
            {"title": "Advanced MongoDB", "description": "Deep dive into text search"},
        ]
    )
    regular_text_collection.create_index(
        [("title", pymongo.TEXT), ("description", pymongo.TEXT)],
        name="regular_text_index_with_weights",
        weights={"title": 5, "description": 1},
        default_language="english",
        language_override="lang",
        textIndexVersion=3,
    )

    # Wildcard Text Index
    wildcard_text_collection = db.wildcard_text_indexes
    wildcard_text_collection.insert_many(
        [
            {"content": "MongoDB wildcard indexing", "extra": "Example document"},
            {"random_field": "This should also be searchable"},
            {"nested": {"field": "Wildcard indexing applies here too"}},
        ]
    )
    wildcard_text_collection.create_index([("$**", pymongo.TEXT)], name="wildcard_text_index")

    # Wildcard Index Variations
    wildcard_collection = db.wildcard_indexes
    wildcard_collection.insert_many(
        [
            {"field1": "value1", "field2": "value2", "nested": {"subfield": "nested_value"}},
            {"field1": "another_value", "extra_field": "extra_data"},
        ]
    )
    wildcard_collection.create_index([("$**", pymongo.ASCENDING)], name="wildcard_index")
    wildcard_collection.create_index(
        [("$**", pymongo.ASCENDING)],
        name="filtered_wildcard_index",
        partialFilterExpression={"extra_field": {"$exists": True}},
    )
    wildcard_collection.create_index(
        [("$**", pymongo.ASCENDING)],
        name="wildcard_projection_index",
        wildcardProjection={"field1": 1, "nested.subfield": 1},
    )

    # Multi-key Index
    multi_key_collection = db.multi_key_indexes
    multi_key_collection.insert_many(
        [
            {"tags": ["mongodb", "database", "index"]},
            {"tags": ["pytest", "testing"]},
            {"tags": ["performance", "optimization"]},
        ]
    )
    multi_key_collection.create_index([("tags", pymongo.ASCENDING)], name="multi_key_index")

    # Clustered Index (MongoDB 5.3+)
    db.create_collection("clustered_collection", clusteredIndex={"key": {"_id": 1}, "unique": True})
    clustered_collection = db.clustered_collection
    clustered_collection.insert_many([{"_id": 1, "name": "Alice"}, {"_id": 2, "name": "Bob"}])

    # Compound Index Variants
    compound_collection = db.compound_indexes
    compound_collection.insert_many(
        [
            {"first_name": "Alice", "last_name": "Smith", "age": 30},
            {"first_name": "Bob", "last_name": "Brown", "age": 25},
        ]
    )
    compound_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)], name="compound_unique_index", unique=True
    )
    compound_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_collation_index",
        collation=pymongo.collation.Collation(locale="en", strength=2),
    )
    compound_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
        name="compound_partial_index",
        partialFilterExpression={"age": {"$gt": 20}},
    )
    compound_collection.create_index(
        [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)], name="compound_sparse_index", sparse=True
    )

    # Hidden Index
    hidden_collection = db.hidden_indexes
    hidden_collection.insert_many([{"data": "example1"}, {"data": "example2"}])
    hidden_collection.create_index([("data", pymongo.ASCENDING)], name="hidden_index", hidden=True)
