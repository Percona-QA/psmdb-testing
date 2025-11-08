import pymongo

def create_sharded_index_types(db, drop_before_creation=False):
    """
    Create sharded-specific index types.
    These indexes are created on sharded collections.
    """
    collections = ["sharded_users", "sharded_orders", "sharded_products"]

    if drop_before_creation:
        for collection in collections:
            if collection in db.list_collection_names():
                db[collection].drop_indexes()

    # Indexes on sharded_users collection
    sharded_users = db.sharded_users
    if "sharded_users" in db.list_collection_names():
        # Secondary index on username (not part of shard key)
        try:
            sharded_users.create_index([("username", pymongo.ASCENDING)], name="username_index")
        except pymongo.errors.OperationFailure:
            pass  # Index might already exist

        # Compound index including shard key
        try:
            sharded_users.create_index([("user_id", pymongo.ASCENDING), ("region", pymongo.ASCENDING)], name="user_region_index")
        except pymongo.errors.OperationFailure:
            pass

        # Text index on email
        try:
            sharded_users.create_index([("email", pymongo.TEXT)], name="email_text_index")
        except pymongo.errors.OperationFailure:
            pass

    # Indexes on sharded_orders collection
    sharded_orders = db.sharded_orders
    if "sharded_orders" in db.list_collection_names():
        # Secondary index on customer_id
        try:
            sharded_orders.create_index([("customer_id", pymongo.ASCENDING)], name="customer_id_index")
        except pymongo.errors.OperationFailure:
            pass

        # Compound index on customer_id and status
        try:
            sharded_orders.create_index([("customer_id", pymongo.ASCENDING), ("status", pymongo.ASCENDING)], name="customer_status_index")
        except pymongo.errors.OperationFailure:
            pass

    # Indexes on sharded_products collection
    sharded_products = db.sharded_products
    if "sharded_products" in db.list_collection_names():
        # Secondary index on name
        try:
            sharded_products.create_index([("name", pymongo.ASCENDING)], name="product_name_index")
        except pymongo.errors.OperationFailure:
            pass

        # Index on price (for range queries)
        try:
            sharded_products.create_index([("price", pymongo.ASCENDING)], name="price_index")
        except pymongo.errors.OperationFailure:
            pass

        # Compound index including shard key fields
        try:
            sharded_products.create_index([("category", pymongo.ASCENDING), ("product_id", pymongo.ASCENDING), ("price", pymongo.ASCENDING)], name="category_product_price_index")
        except pymongo.errors.OperationFailure:
            pass
