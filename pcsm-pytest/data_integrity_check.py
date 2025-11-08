import json
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from cluster import Cluster

def compare_data(db1, db2):
    """
    Unified function to compare data between two MongoDB setups,
    works with both replica set and sharded clusters
    """
    def resolve_container_or_uri(db):
        if hasattr(db, "connection"):
            return db.connection
        elif isinstance(db, str) and (db.startswith("mongodb://") or db.startswith("mongodb+srv://")):
            return db
        return None

    db1_container = resolve_container_or_uri(db1)
    db2_container = resolve_container_or_uri(db2)
    if db1_container is None or db2_container is None:
        raise ValueError("Invalid database argument: must be cluster object or connection string")

    is_sharded = False
    if hasattr(db1, "layout") and db1.layout == "sharded":
        is_sharded = True
    elif hasattr(db2, "layout") and db2.layout == "sharded":
        is_sharded = True

    mismatch_summary = []
    # Hash mismatch is only checked for replica sets, not sharded clusters
    if not is_sharded:
        all_coll_hash, mismatch_dbs_hash, mismatch_coll_hash = compare_database_hashes(db1_container, db2_container)
        if mismatch_dbs_hash:
            mismatch_summary.extend(mismatch_dbs_hash)
        if mismatch_coll_hash:
            mismatch_summary.extend(mismatch_coll_hash)

    all_collections, mismatch_dbs_count, mismatch_coll_count = compare_entries_number(db1_container, db2_container)
    mismatch_metadata = compare_collection_metadata(db1_container, db2_container)
    mismatch_indexes = compare_collection_indexes(db1_container, db2_container, all_collections)

    if mismatch_dbs_count:
        mismatch_summary.extend(mismatch_dbs_count)
    if mismatch_coll_count:
        mismatch_summary.extend(mismatch_coll_count)
    if mismatch_metadata:
        mismatch_summary.extend(mismatch_metadata)
    if mismatch_indexes:
        mismatch_summary.extend(mismatch_indexes)

    if is_sharded:
        mismatch_sharding = compare_collection_sharding(db1_container, db2_container, all_collections)
        if mismatch_sharding:
            mismatch_summary.extend(mismatch_sharding)

    if not mismatch_summary:
        Cluster.log("Data and indexes are consistent between source and destination databases")
        return True, []

    Cluster.log(f"Mismatched databases, collections, or indexes found: {mismatch_summary}")
    return False, mismatch_summary

def compare_database_hashes(db1_container, db2_container):
    def get_db_hashes_and_collections(uri):
        client = MongoClient(uri)
        db_hashes = {}
        collection_hashes = {}

        for db_name in client.list_database_names():
            if db_name in ["admin", "local", "config", "percona_clustersync_mongodb"]:
                continue
            db = client[db_name]
            collections = [name for name in db.list_collection_names()]
            try:
                if collections:
                    result = db.command("dbHash", collections=collections)
                else:
                    result = db.command("dbHash")
                db_hashes[db_name] = result.get("md5")
                for coll, coll_hash in result.get("collections", {}).items():
                    collection_hashes[f"{db_name}.{coll}"] = coll_hash
            except PyMongoError as e:
                Cluster.log(f"Warning: could not run dbHash on {db_name}: {str(e)}")
                db_hashes[db_name] = None
        return db_hashes, collection_hashes

    db1_hashes, db1_collections = get_db_hashes_and_collections(db1_container)
    db2_hashes, db2_collections = get_db_hashes_and_collections(db2_container)

    Cluster.log("Comparing database hashes...")
    mismatched_dbs = []
    for db_name in db1_hashes:
        if db_name not in db2_hashes:
            mismatched_dbs.append((db_name, "missing in dst DB"))
            Cluster.log(f"Database '{db_name}' exists in source_DB but not in destination_DB")
        elif db1_hashes[db_name] != db2_hashes[db_name]:
            mismatched_dbs.append((db_name, "hash mismatch"))
            Cluster.log(f"Database '{db_name}' hash mismatch: {db1_hashes[db_name]} != {db2_hashes[db_name]}")

    for db_name in db2_hashes:
        if db_name not in db1_hashes:
            mismatched_dbs.append((db_name, "missing in src DB"))
            Cluster.log(f"Database '{db_name}' exists in destination_DB but not in source_DB")

    Cluster.log("Comparing collection hashes...")
    mismatched_collections = []
    for coll_name in db1_collections:
        if coll_name not in db2_collections:
            mismatched_collections.append((coll_name, "missing in dst DB"))
            Cluster.log(f"Collection '{coll_name}' exists in source_DB but not in destination_DB")
        elif db1_collections[coll_name] != db2_collections[coll_name]:
            mismatched_collections.append((coll_name, "hash mismatch"))
            Cluster.log(f"Collection '{coll_name}' hash mismatch: {db1_collections[coll_name]} != {db2_collections[coll_name]}")

    for coll_name in db2_collections:
        if coll_name not in db1_collections:
            mismatched_collections.append((coll_name, "missing in src DB"))
            Cluster.log(f"Collection '{coll_name}' exists in destination_DB but not in source_DB")

    return db1_collections.keys() | db2_collections.keys(), mismatched_dbs, mismatched_collections

def compare_entries_number(db1_container, db2_container):
    def get_collection_counts(uri):
        client = MongoClient(uri)
        collection_counts = {}
        for db_name in client.list_database_names():
            if db_name in ["admin", "local", "config", "percona_clustersync_mongodb"]:
                continue
            db = client[db_name]
            for coll_name in db.list_collection_names():
                if coll_name.startswith("system."):
                    continue
                try:
                    collection_counts[f"{db_name}.{coll_name}"] = db[coll_name].count_documents({})
                except Exception as e:
                    Cluster.log(f"Warning: Could not count documents in {db_name}.{coll_name}: {e}")
                    continue
        return collection_counts

    db1_counts = get_collection_counts(db1_container)
    db2_counts = get_collection_counts(db2_container)

    Cluster.log("Comparing collection record counts...")
    mismatched_dbs = []
    mismatched_collections = []

    for coll_name in db1_counts:
        if coll_name not in db2_counts:
            mismatched_collections.append((coll_name, "missing in dst DB"))
            Cluster.log(f"Collection '{coll_name}' exists in source_DB but not in destination_DB")
        elif db1_counts[coll_name] != db2_counts[coll_name]:
            mismatched_collections.append((coll_name, "record count mismatch"))
            Cluster.log(f"Collection '{coll_name}' record count mismatch: {db1_counts[coll_name]} != {db2_counts[coll_name]}")

    for coll_name in db2_counts:
        if coll_name not in db1_counts:
            mismatched_collections.append((coll_name, "missing in src DB"))
            Cluster.log(f"Collection '{coll_name}' exists in destination_DB but not in source_DB")

    return db1_counts.keys() | db2_counts.keys(), mismatched_dbs, mismatched_collections

def compare_collection_metadata(db1_container, db2_container):
    Cluster.log("Comparing collection metadata...")
    mismatched_metadata = []

    db1_metadata = get_all_collection_metadata(db1_container)
    db2_metadata = get_all_collection_metadata(db2_container)

    db1_collections = {f"{coll['db']}.{coll['name']}": coll for coll in db1_metadata}
    db2_collections = {f"{coll['db']}.{coll['name']}": coll for coll in db2_metadata}

    all_collections = set(db1_collections.keys()).union(set(db2_collections.keys()))

    for coll_name in all_collections:
        if coll_name not in db2_collections:
            mismatched_metadata.append((coll_name, "missing in dst DB"))
            Cluster.log(f"Collection '{coll_name}' exists in source_DB but not in destination_DB")
            continue

        if coll_name not in db1_collections:
            mismatched_metadata.append((coll_name, "missing in src DB"))
            Cluster.log(f"Collection '{coll_name}' exists in destination_DB but not in source_DB")
            continue

        for field in ["type", "options", "idIndex"]:
            if db1_collections[coll_name].get(field) != db2_collections[coll_name].get(field):
                mismatched_metadata.append((coll_name, f"{field} mismatch"))
                Cluster.log(f"Collection '{coll_name}' has different {field} in source and destination.")
                Cluster.log(f"Source_DB: {json.dumps(db1_collections[coll_name].get(field), indent=2)}")
                Cluster.log(f"Destination_DB: {json.dumps(db2_collections[coll_name].get(field), indent=2)}")

    return mismatched_metadata

def get_all_collection_metadata(uri):
    client = MongoClient(uri)
    metadata_list = []

    for db_name in client.list_database_names():
        if db_name in ["admin", "local", "config", "percona_clustersync_mongodb"]:
            continue
        db = client[db_name]
        try:
            for coll in db.list_collections():
                metadata_list.append({
                    "db": db_name,
                    "name": coll["name"],
                    "type": coll.get("type"),
                    "options": coll.get("options"),
                    "idIndex": coll.get("idIndex")
                })
        except PyMongoError as e:
            Cluster.log(f"Warning: Could not access metadata for DB '{db_name}': {str(e)}")
            return []
    return metadata_list

def compare_collection_indexes(db1_container, db2_container, all_collections):
    Cluster.log("Comparing collection indexes...")
    mismatched_indexes = []

    for coll_name in all_collections:
        db1_indexes = get_indexes(db1_container, coll_name)
        db2_indexes = get_indexes(db2_container, coll_name)

        db1_index_dict = {index["name"]: index for index in db1_indexes if "name" in index}
        db2_index_dict = {index["name"]: index for index in db2_indexes if "name" in index}

        for index_name, index_details in db1_index_dict.items():
            if index_name not in db2_index_dict:
                mismatched_indexes.append((coll_name, index_name))
                Cluster.log(f"Collection '{coll_name}': Index '{index_name}' exists in source_DB but not in destination_DB")

        for index_name in db2_index_dict.keys():
            if index_name not in db1_index_dict:
                mismatched_indexes.append((coll_name, index_name))
                Cluster.log(f"Collection '{coll_name}': Index '{index_name}' exists in destination_DB but not in source_DB")

        for index_name in set(db1_index_dict.keys()).intersection(db2_index_dict.keys()):
            index1 = db1_index_dict[index_name]
            index2 = db2_index_dict[index_name]

            fields_to_compare = [
                "key", "unique", "sparse", "hidden", "storageEngine", "collation",
                "partialFilterExpression", "expireAfterSeconds", "weights",
                "default_language", "language_override", "textIndexVersion",
                "2dsphereIndexVersion", "bits", "min", "max", "wildcardProjection"
            ]

            index1_filtered = {k: index1[k] for k in fields_to_compare if k in index1}
            index2_filtered = {k: index2[k] for k in fields_to_compare if k in index2}

            if index1_filtered != index2_filtered:
                mismatched_indexes.append((coll_name, index_name))
                Cluster.log(f"Collection '{coll_name}': Index '{index_name}' differs in structure.")
                Cluster.log(f"Source_DB: {json.dumps(index1_filtered, indent=2)}")
                Cluster.log(f"Destination_DB: {json.dumps(index2_filtered, indent=2)}")

    return mismatched_indexes

def get_indexes(uri, collection_name):
    db_name, coll_name = collection_name.split(".", 1)

    client = MongoClient(uri)
    try:
        indexes = list(client[db_name][coll_name].list_indexes())
        return sorted([
            {
                "name": index.get("name"),
                "key": index.get("key"),
                "unique": index.get("unique", False),
                "sparse": index.get("sparse", False),
                "hidden": index.get("hidden", False),
                "storageEngine": index.get("storageEngine"),
                "collation": index.get("collation"),
                "partialFilterExpression": index.get("partialFilterExpression"),
                "expireAfterSeconds": index.get("expireAfterSeconds"),
                "weights": index.get("weights"),
                "default_language": index.get("default_language"),
                "language_override": index.get("language_override"),
                "textIndexVersion": index.get("textIndexVersion"),
                "2dsphereIndexVersion": index.get("2dsphereIndexVersion"),
                "bits": index.get("bits"),
                "min": index.get("min"),
                "max": index.get("max"),
                "wildcardProjection": index.get("wildcardProjection"),
            }
            for index in indexes if "key" in index and "name" in index
        ], key=lambda x: x["name"])
    except PyMongoError:
        return []

def compare_collection_sharding(db1_container, db2_container, all_collections):
    Cluster.log("Comparing collection sharding information...")
    mismatched_sharding = []

    def get_sharding_info(uri):
        client = MongoClient(uri)
        sharding_info = {}
        try:
            config_db = client.get_database("config")
            collection_names = config_db.list_collection_names()
            if "collections" in collection_names:
                for coll_doc in config_db["collections"].find({}):
                    ns = coll_doc.get("_id")
                    if ns:
                        sharding_info[ns] = {
                            "key": coll_doc.get("key"),
                            "unique": coll_doc.get("unique", False)}
            else:
                Cluster.log("Warning: No sharded collections found")
        except PyMongoError as e:
            Cluster.log(f"Warning: Could not access sharding info: {str(e)}")
        return sharding_info

    db1_sharding = get_sharding_info(db1_container)
    db2_sharding = get_sharding_info(db2_container)

    for coll_name in all_collections:
        db1_info = db1_sharding.get(coll_name)
        db2_info = db2_sharding.get(coll_name)

        db1_is_sharded = db1_info is not None and db1_info.get("key") is not None
        db2_is_sharded = db2_info is not None and db2_info.get("key") is not None

        if db1_is_sharded != db2_is_sharded:
            mismatched_sharding.append((coll_name, "sharding status mismatch"))
            Cluster.log(f"Collection '{coll_name}': sharded={db1_is_sharded} in source, sharded={db2_is_sharded} in destination")
            continue

        if db1_is_sharded and db2_is_sharded:
            db1_key = db1_info.get("key")
            db2_key = db2_info.get("key")
            if db1_key != db2_key:
                mismatched_sharding.append((coll_name, "shard key mismatch"))
                Cluster.log(f"Collection '{coll_name}': shard key mismatch")
                Cluster.log(f"Source_DB shard key: {json.dumps(db1_key, indent=2)}")
                Cluster.log(f"Destination_DB shard key: {json.dumps(db2_key, indent=2)}")

            db1_unique = db1_info.get("unique", False)
            db2_unique = db2_info.get("unique", False)
            if db1_unique != db2_unique:
                mismatched_sharding.append((coll_name, "shard key unique flag mismatch"))
                Cluster.log(f"Collection '{coll_name}': shard key unique flag mismatch: {db1_unique} != {db2_unique}")

    return mismatched_sharding