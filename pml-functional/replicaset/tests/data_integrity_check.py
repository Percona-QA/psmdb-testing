import os
import sys

import docker
import json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from mlink.cluster import Cluster

def compare_data_rs(db1, db2, port):

    all_coll_hash, mismatch_dbs_hash, mismatch_coll_hash = compare_database_hashes(db1, db2, port)
    all_coll_count, mismatch_dbs_count, mismatch_coll_count = compare_entries_number(db1, db2, port)
    mismatch_metadata = compare_collection_metadata(db1, db2, port)
    mismatch_indexes = compare_collection_indexes(db1, db2, all_coll_hash, port)

    mismatch_summary = []

    # if mismatch_dbs_hash:
    #     mismatch_summary.extend(mismatch_dbs_hash)
    # if mismatch_coll_hash:
    #     mismatch_summary.extend(mismatch_coll_hash)
    # if mismatch_dbs_count:
    #     mismatch_summary.extend(mismatch_dbs_count)
    # if mismatch_coll_count:
    #     mismatch_summary.extend(mismatch_coll_count)
    if mismatch_metadata:
        mismatch_summary.extend(mismatch_metadata)
    if mismatch_indexes:
        mismatch_summary.extend(mismatch_indexes)

    if not mismatch_summary:
        Cluster.log("Data and indexes are consistent between source and destination databases")
        return True, []

    Cluster.log(f"Mismatched databases, collections, or indexes found: {mismatch_summary}")
    return False, mismatch_summary

def compare_database_hashes(db1, db2, port):
    query = (
        'db.getMongo().getDBNames().forEach(function(dbName) { '
        'if (!["admin", "local", "config", "percona_mongolink"].includes(dbName)) { '
        'var collections = []; '
        'db.getSiblingDB(dbName).runCommand({ listCollections: 1 }).cursor.firstBatch.forEach(function(coll) { '
        'if (!coll.type || coll.type !== "view") { collections.push(coll.name); } '
        '}); '
        'if (collections.length > 0) { '
        'var result = db.getSiblingDB(dbName).runCommand({ dbHash: 1, collections: collections }); '
        'print(JSON.stringify({db: dbName, md5: result.md5, collections: result.collections})); '
        '} else { '
        'print(JSON.stringify({db: dbName, md5: null, collections: {}})); '
        '} '
        '} '
        '});'
    )

    def get_db_hashes_and_collections(db):
        response = db.check_output("mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval '" + query + "' --quiet")

        db_hashes = {}
        collection_hashes = {}

        for line in response.split("\n"):
            try:
                db_info = json.loads(line)
                db_name = db_info["db"]
                db_hashes[db_name] = db_info["md5"]

                for coll, coll_hash in db_info["collections"].items():
                    collection_hashes[f"{db_name}.{coll}"] = coll_hash
            except json.JSONDecodeError:
                Cluster.log(f"Warning: Skipping invalid JSON line: {line}")

        return db_hashes, collection_hashes

    db1_hashes, db1_collections = get_db_hashes_and_collections(db1)
    db2_hashes, db2_collections = get_db_hashes_and_collections(db2)

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

def compare_entries_number(db1, db2, port):
    query = (
        'db.getMongo().getDBNames().forEach(function(i) { '
        'if (!["admin", "local", "config", "percona_mongolink"].includes(i)) { '
        'var collections = db.getSiblingDB(i).runCommand({ listCollections: 1 }).cursor.firstBatch '
        '.filter(function(coll) { return !coll.type || coll.type !== "view"; }) '
        '.map(function(coll) { return coll.name; }); '
        'collections.forEach(function(coll) { '
        'try { '
        'var count = db.getSiblingDB(i).getCollection(coll).countDocuments({}); '
        'print(JSON.stringify({db: i, collection: coll, count: count})); '
        '} catch (err) {} '
        '});}});'
    )

    def get_collection_counts(db):
        response = db.check_output("mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval '" + query + "' --quiet")

        collection_counts = {}

        for line in response.split("\n"):
            try:
                count_info = json.loads(line)
                collection_name = f"{count_info['db']}.{count_info['collection']}"
                collection_counts[collection_name] = count_info["count"]
            except json.JSONDecodeError:
                Cluster.log(f"Warning: Skipping invalid JSON line: {line}")

        return collection_counts

    db1_counts = get_collection_counts(db1)
    db2_counts = get_collection_counts(db2)

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

def compare_collection_metadata(db1, db2, port):
    Cluster.log("Comparing collection metadata...")
    mismatched_metadata = []

    db1_metadata = get_all_collection_metadata(db1, port)
    db2_metadata = get_all_collection_metadata(db2, port)

    db1_collections = {f"{coll['db']}.{coll['name']}": coll for coll in db1_metadata}
    db2_collections = {f"{coll['db']}.{coll['name']}": coll for coll in db2_metadata}

    all_collections = set(db1_collections.keys()).union(set(db2_collections.keys()))

    for coll_name in all_collections:
        if coll_name not in db2_collections:
            mismatched_metadata.append((coll_name, "missing in dst DB"))
            Cluster.log(f"Collection '{coll_name}' exists in source DB but not in destination DB")
            continue

        if coll_name not in db1_collections:
            mismatched_metadata.append((coll_name, "missing in src DB"))
            Cluster.log(f"Collection '{coll_name}' exists in destination DB but not in source DB")
            continue

        for field in ["type", "options", "idIndex"]:
            if db1_collections[coll_name].get(field) != db2_collections[coll_name].get(field):
                mismatched_metadata.append((coll_name, f"{field} mismatch"))
                Cluster.log(f"Collection '{coll_name}' has different {field} in source and destination.")
                Cluster.log(f"Source DB: {json.dumps(db1_collections[coll_name].get(field), indent=2)}")
                Cluster.log(f"Destination DB: {json.dumps(db2_collections[coll_name].get(field), indent=2)}")

    return mismatched_metadata

def get_all_collection_metadata(db, port):
    query = (
        'db.getMongo().getDBNames().forEach(function(i) { '
        'if (!["admin", "local", "config", "percona_mongolink"].includes(i)) { '
        'var collections = db.getSiblingDB(i).runCommand({ listCollections: 1 }).cursor.firstBatch '
        '.map(function(coll) { return { db: i, name: coll.name, type: coll.type, options: coll.options }; }); '
        'print(JSON.stringify(collections)); }});'
    )

    response = db.check_output(
        "mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval '" + query + "' --quiet")

    try:
        metadata_list = []
        for line in response.splitlines():
            metadata_list.extend(json.loads(line))
        return metadata_list
    except json.JSONDecodeError:
        Cluster.log(f"Error: Unable to parse JSON collection metadata response")
        Cluster.log(f"Raw response: {response}")
        return []

def compare_collection_indexes(db1, db2, all_collections, port):
    Cluster.log("Comparing collection indexes...")
    mismatched_indexes = []

    for coll_name in all_collections:
        db1_indexes = get_indexes(db1, coll_name, port)
        db2_indexes = get_indexes(db2, coll_name, port)

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

def get_indexes(db, collection_name, port):
    db_name, coll_name = collection_name.split(".", 1)

    print("KEITH TEST: " + db_name + ":" + coll_name)

    query = f'db.getSiblingDB("{db_name}").getCollection("{coll_name}").getIndexes()'
    response = db.check_output(
        "mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --json --eval '" + query + "' --quiet")

    print("KEITH TEST: " + str(response))

    try:
        indexes = json.loads(response)

        print("KEITH TEST: " + str(indexes))

        def normalize_key(index_key):
            print("KEITH TEST: 123")
            if isinstance(index_key, dict):
                return {k: normalize_key(v) for k, v in index_key.items()}
            elif isinstance(index_key, list):
                return [normalize_key(v) for v in index_key]
            elif isinstance(index_key, dict) and "$numberInt" in index_key:
                return int(index_key["$numberInt"])
            print("KEITH TEST: 124")
            return index_key

        return sorted([
            {
                "name": index.get("name"),
                "key": normalize_key(index.get("key")),
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

    except json.JSONDecodeError:
        Cluster.log(f"Error: Unable to parse JSON index response for {collection_name}")
        Cluster.log(f"Raw response: {response}")
        return []
