import pymongo
import datetime

def create_sharded_index_types(db, drop_before_creation=False):
    collections = [
        "sharded_geo_indexes", "sharded_hashed_indexes", "sharded_ttl_indexes", "sharded_partial_indexes",
        "sharded_text_indexes", "sharded_regular_text_indexes", "sharded_wildcard_text_indexes",
        "sharded_wildcard_indexes", "sharded_multi_key_indexes",
        "sharded_compound_indexes", "sharded_hidden_indexes"
    ]

    if drop_before_creation:
        for collection in collections:
            if collection in db.list_collection_names():
                db[collection].drop_indexes()
                db.drop_collection(collection)

    db.client.admin.command("enableSharding", db.name)

    # Geospatial index on sharded collection
    sharded_geo_collection = db.sharded_geo_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_geo_indexes", key={"_id": "hashed"})
    sharded_geo_collection.insert_many([
        {"_id": i, "location_2d": [-122.4194 + i * 0.1, 37.7749 + i * 0.1],
        "location_2dsphere": {"type": "Point", "coordinates": [-74.0060 + i * 0.1, 40.7128 + i * 0.1]}}
        for i in range(20)])
    sharded_geo_collection.create_index([("location_2d", pymongo.GEO2D)], name="2d_index", min=-180, max=180, bits=32)
    sharded_geo_collection.create_index([("location_2dsphere", pymongo.GEOSPHERE)], name="2dsphere_index")

    # Hashed index on sharded collection
    sharded_hashed_collection = db.sharded_hashed_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_hashed_indexes", key={"_id": "hashed"})
    sharded_hashed_collection.insert_many([
        {"_id": i, "hashed_field": f"user_{i}", "secondary_field": f"extra_{i}"} for i in range(20)])
    sharded_hashed_collection.create_index([("hashed_field", pymongo.HASHED)], name="hashed_basic_index")
    sharded_hashed_collection.create_index([("hashed_field", pymongo.HASHED)], name="hashed_partial_index",
                                   partialFilterExpression={"secondary_field": {"$exists": True}})
    sharded_hashed_collection.create_index([("hashed_field", pymongo.HASHED)], name="hashed_sparse_index", sparse=True)
    sharded_hashed_collection.create_index([("hashed_field", pymongo.HASHED), ("secondary_field", pymongo.ASCENDING)],
                                   name="hashed_compound_index")

    # TTL index on sharded collection
    sharded_ttl_collection = db.sharded_ttl_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_ttl_indexes", key={"_id": "hashed"})
    sharded_ttl_collection.insert_many([
        {"_id": i, "created_at": datetime.datetime.now(datetime.timezone.utc), "short_lived": i % 2 == 0, "long_lived": i % 2 == 1}
        for i in range(20)])
    sharded_ttl_collection.create_index([("created_at", pymongo.ASCENDING)], name="ttl_index", expireAfterSeconds=3600)
    sharded_ttl_collection.create_index([("created_at", pymongo.ASCENDING)], name="ttl_partial_index",
                                expireAfterSeconds=7200, partialFilterExpression={"short_lived": True})

    # Partial index on sharded collection
    sharded_partial_collection = db.sharded_partial_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_partial_indexes", key={"_id": "hashed"})
    sharded_partial_collection.insert_many([
        {"_id": i, "partial_field": "indexed" if i % 2 == 0 else None, "non_partial_field": "not indexed" if i % 2 == 1 else None}
        for i in range(20)])
    sharded_partial_collection.create_index(
        [("partial_field", pymongo.ASCENDING)],
        name="partial_index", partialFilterExpression={"partial_field": {"$exists": True}})

    # Regular text index on sharded collection
    sharded_text_collection = db.sharded_text_indexes
    sharded_text_collection.create_index([("extra", pymongo.ASCENDING)])
    db.client.admin.command("shardCollection", f"{db.name}.sharded_text_indexes", key={"extra": 1})
    sharded_text_collection.insert_many([
        {"_id": 0, "content": "Hello MongoDB", "extra": "Some extra data"},
        {"_id": 1, "content": "Pytest integration testing", "extra": "Another document"}])
    sharded_text_collection.create_index([("extra", pymongo.ASCENDING), ("content", pymongo.TEXT)], name="regular_text_index", unique=True)

    # Regular text index with weights on sharded collection
    sharded_regular_text_collection = db.sharded_regular_text_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_regular_text_indexes", key={"_id": "hashed"})
    sharded_regular_text_collection.insert_many([
        {"_id": i, "title": f"MongoDB Basics {i}", "description": f"A guide to MongoDB indexes {i}"}
        for i in range(20)])
    sharded_regular_text_collection.create_index(
        [("title", pymongo.TEXT), ("description", pymongo.TEXT)],
        name="regular_text_index_with_weights",
        weights={"title": 5, "description": 1},
        default_language="english",
        language_override="lang",
        textIndexVersion=3)

    # Wildcard text index on sharded collection
    sharded_wildcard_text_collection = db.sharded_wildcard_text_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_wildcard_text_indexes", key={"_id": "hashed"})
    wildcard_text_docs = []
    for i in range(10):
        wildcard_text_docs.append({"_id": i, "content": f"MongoDB wildcard indexing {i}", "extra": f"Example document {i}"})
        wildcard_text_docs.append({"_id": i + 20, "random_field": f"This should also be searchable {i}"})
        wildcard_text_docs.append({"_id": i + 40, "nested": {"field": f"Wildcard indexing applies here too {i}"}})
    sharded_wildcard_text_collection.insert_many(wildcard_text_docs)
    sharded_wildcard_text_collection.create_index([("$**", pymongo.TEXT)], name="wildcard_text_index")

    # Wildcard index on sharded collection
    sharded_wildcard_collection = db.sharded_wildcard_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_wildcard_indexes", key={"_id": "hashed"})
    wildcard_docs = []
    for i in range(10):
        wildcard_docs.append({"_id": i, "field1": f"value1_{i}", "field2": f"value2_{i}", "nested": {"subfield": f"nested_value_{i}"}})
        wildcard_docs.append({"_id": i + 20, "field1": f"another_value_{i}", "extra_field": f"extra_data_{i}"})
    sharded_wildcard_collection.insert_many(wildcard_docs)
    sharded_wildcard_collection.create_index([("$**", pymongo.ASCENDING)], name="wildcard_index")
    sharded_wildcard_collection.create_index(
        [("$**", pymongo.ASCENDING)], name="filtered_wildcard_index",
        partialFilterExpression={"extra_field": {"$exists": True}})
    sharded_wildcard_collection.create_index(
        [("$**", pymongo.ASCENDING)], name="wildcard_projection_index",
        wildcardProjection={"field1": 1, "nested.subfield": 1})

    # Multi-key index on sharded collection
    sharded_multi_key_collection = db.sharded_multi_key_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_multi_key_indexes", key={"_id": "hashed"})
    multi_key_docs = []
    for i in range(10):
        multi_key_docs.append({"_id": i, "tags": [f"mongodb_{i}", f"database_{i}", f"index_{i}"]})
        multi_key_docs.append({"_id": i + 20, "tags": [f"pytest_{i}", f"testing_{i}"]})
        multi_key_docs.append({"_id": i + 40, "tags": [f"performance_{i}", f"optimization_{i}"]})
    sharded_multi_key_collection.insert_many(multi_key_docs)
    sharded_multi_key_collection.create_index([("tags", pymongo.ASCENDING)], name="multi_key_index")

    # Compound index on sharded collection
    sharded_compound_collection = db.sharded_compound_indexes
    sharded_compound_collection.create_index([("age", pymongo.ASCENDING)])
    db.client.admin.command("shardCollection", f"{db.name}.sharded_compound_indexes", key={"age": 1})
    sharded_compound_collection.insert_many([
        {"_id": i, "first_name": f"Alice_{i}", "last_name": f"Smith_{i}", "age": 30 + i}
        for i in range(20)])
    sharded_compound_collection.create_index([("age", pymongo.ASCENDING), ("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
                                     name="compound_unique_index", unique=True)
    sharded_compound_collection.create_index( [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
                                     name="compound_collation_index", collation=pymongo.collation.Collation(locale="en", strength=2))
    sharded_compound_collection.create_index( [("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
                                     name="compound_partial_index", partialFilterExpression={"age": {"$gt": 20}})
    sharded_compound_collection.create_index([("first_name", pymongo.ASCENDING), ("last_name", pymongo.ASCENDING)],
                                     name="compound_sparse_index", sparse=True)

    # Hidden index on sharded collection
    sharded_hidden_collection = db.sharded_hidden_indexes
    db.client.admin.command("shardCollection", f"{db.name}.sharded_hidden_indexes", key={"_id": "hashed"})
    sharded_hidden_collection.insert_many([{"_id": i, "data": f"example{i}"} for i in range(20)])
    sharded_hidden_collection.create_index([("data", pymongo.ASCENDING)], name="hidden_index", hidden=True)