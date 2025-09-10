from gridfs import GridFS

def create_diff_coll_types(db, drop_before_creation=False):
    if drop_before_creation:
        for collection_name in ["customers", "purchases", "large_docs"]:
            db.drop_collection(collection_name)
        for view_name in ["customer_view", "active_customers", "customer_purchases", "high_value_purchases"]:
            db.drop_collection(view_name)
        db.drop_collection("fs.files")
        db.drop_collection("fs.chunks")

    if "customers" not in db.list_collection_names():
        db["customers"].insert_many([
            {"_id": 1, "name": "Alice", "status": "active", "age": 30},
            {"_id": 2, "name": "Bob", "status": "inactive", "age": 25},
            {"_id": 3, "name": "Charlie", "status": "active", "age": 35}
        ])

    if "purchases" not in db.list_collection_names():
        db["purchases"].insert_many([
            {"_id": 101, "customer_id": 1, "total": 100.0},
            {"_id": 102, "customer_id": 1, "total": 200.5},
            {"_id": 103, "customer_id": 3, "total": 50.0},
            {"_id": 104, "customer_id": 3, "total": 500.0}
        ])

    # Simple view
    db.command({
        "create": "customer_view",
        "viewOn": "customers",
        "pipeline": [{"$match": {"status": "active"}}]
    })

    # Aggregation view
    db.command({
        "create": "active_customers",
        "viewOn": "customers",
        "pipeline": [
            {"$match": {"status": "active"}},
            {"$project": {"_id": 1, "name": 1, "age_group": {"$cond": [{"$gte": ["$age", 30]}, "Senior", "Junior"]}}}
        ]
    })

    # Lookup view
    db.command({
        "create": "customer_purchases",
        "viewOn": "customers",
        "pipeline": [
            {"$lookup": {
                "from": "purchases",
                "localField": "_id",
                "foreignField": "customer_id",
                "as": "customer_orders"
            }},
            {"$project": {"_id": 1, "name": 1, "customer_orders": 1}}
        ]
    })

    # Non-materialized view
    db.command({
        "create": "high_value_purchases",
        "viewOn": "purchases",
        "pipeline": [
            {"$match": {"total": {"$gt": 100.0}}},
            {"$lookup": {
                "from": "customers",
                "localField": "customer_id",
                "foreignField": "_id",
                "as": "customer_info"
            }},
            {"$project": {"_id": 1, "customer_id": 1, "total": 1, "customer_info.name": 1}}
        ]
    })

    # Collection with large document (~16MB)
    large_doc_collection = db["large_docs"]
    large_doc = {
        "_id": 1,
        "name": "Large Document Test",
        "data": "x" * 16777161
    }
    large_doc_collection.insert_one(large_doc)

    # GridFS
    fs = GridFS(db)
    fs.put(b"BinaryData" * 1000000, filename="large_file.txt")