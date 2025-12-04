import json
import os
import sys

# Add the replicaset tests directory to sys.path
CURRENT_DIR = os.path.dirname(__file__)
REPLICASET_TESTS = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "replicaset", "tests")
)
if REPLICASET_TESTS not in sys.path:
    sys.path.insert(0, REPLICASET_TESTS)

def compare_data_rs(db1, db2, port, full_comparison, state):

    all_coll_count, mismatch_dbs_count, mismatch_coll_count = compare_entries_number(db1, db2, port, state)
    mismatch_summary = []

    if mismatch_dbs_count:
        mismatch_summary.extend(mismatch_dbs_count)
    if mismatch_coll_count:
        mismatch_summary.extend(mismatch_coll_count)

    if full_comparison:
        all_coll_hash, mismatch_dbs_hash, mismatch_coll_hash = compare_database_hashes(db1, db2, port, state)
        mismatch_metadata = compare_collection_metadata(db1, db2, port, state)
        mismatch_indexes = compare_collection_indexes(db1, db2, all_coll_hash, port, state)

        if mismatch_dbs_hash:
            mismatch_summary.extend(mismatch_dbs_hash)
        if mismatch_coll_hash:
            mismatch_summary.extend(mismatch_coll_hash)
        if mismatch_metadata:
            mismatch_summary.extend(mismatch_metadata)
        if mismatch_indexes:
            mismatch_summary.extend(mismatch_indexes)

    if not mismatch_summary:
        print("Data and indexes are consistent between source and destination databases")
        return True, []

    print(f"Mismatched databases, collections, or indexes found: {mismatch_summary}")
    return False, mismatch_summary

def compare_database_hashes(db1, db2, port, state):
    if state != "sharded":
        # Original single-RS / standalone query
        query = (
            'db.getMongo().getDBNames().forEach(function(dbName) { '
            '    if (!["admin", "local", "config", "percona_clustersync_mongodb"].includes(dbName)) { '
            '        var collections = []; '
            '        db.getSiblingDB(dbName).runCommand({ listCollections: 1 }).cursor.firstBatch.forEach(function(coll) { '
            '            if ((!coll.type || coll.type !== "view") && !(dbName === "test" && coll.name === "system.profile")) { '
            '                collections.push(coll.name); '
            '            } '
            '        }); '
            '        if (collections.length > 0) { '
            '            var result = db.getSiblingDB(dbName).runCommand({ dbHash: 1, collections: collections }); '
            '            print(JSON.stringify({ db: dbName, md5: result.md5, collections: result.collections })); '
            '        } else { '
            '            print(JSON.stringify({ db: dbName, md5: null, collections: {} })); '
            '        } '
            '    } '
            '});'
        )
    else:
        # Sharded query (Option A) – run via mongos, but dbHash per shard
        query = (
            'var ignoredDbs = ["admin", "local", "config", "percona_clustersync_mongodb"]; '
            'var conn = db.getMongo(); '
            'var configDb = conn.getDB("config"); '
            'var shards = configDb.shards.find().toArray(); '
            'conn.getDBNames().forEach(function(dbName) { '
            '  if (ignoredDbs.includes(dbName)) { return; } '
            '  var collections = []; '
            '  conn.getDB(dbName).runCommand({ listCollections: 1 }).cursor.firstBatch.forEach(function(coll) { '
            '    if ((!coll.type || coll.type !== "view") && !(dbName === "test" && coll.name === "system.profile")) { '
            '      collections.push(coll.name); '
            '    } '
            '  }); '
            '  shards.forEach(function(shardDoc) { '
            '    var shardName = shardDoc._id; '
            '    var shardHost = shardDoc.host; '
            '    var seed = shardHost.indexOf("/") !== -1 ? shardHost.split("/")[1] : shardHost; '
            '    var firstMember = seed.split(",")[0]; '
            '    try { '
            '      var shardConn = new Mongo(firstMember); '
            '      var shardDb = shardConn.getDB(dbName); '
            '      var result = shardDb.runCommand({ dbHash: 1, collections: collections }); '
            '      print(JSON.stringify({ db: dbName, shard: shardName, md5: result.md5, collections: result.collections })); '
            '    } catch (err) { '
            '      print(JSON.stringify({ db: dbName, shard: shardDoc._id, md5: null, collections: {} })); '
            '    } '
            '  }); '
            '});'
        )

    def get_db_hashes_and_collections(db, state):
        if state != "sharded":
            response = db.check_output(
                "mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval '" + query + "' --quiet")
        else:
            response = db.check_output(
                "mongo mongodb://127.0.0.1:27018/test --eval '" + query + "' --quiet")

        db_hashes = {}
        collection_hashes = {}

        for line in response.split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                db_info = json.loads(line)
            except json.JSONDecodeError:
                print(f"Warning: Skipping invalid JSON line: {line}")
                continue

            db_name = db_info["db"]
            collections = db_info.get("collections", {})

            if state == "sharded" and not collections:
                continue

            db_hashes[db_name] = db_info["md5"]

            for coll, coll_hash in db_info.get("collections", {}).items():
                coll_key = f"{db_name}.{coll}"
                collection_hashes[coll_key] = coll_hash

        return db_hashes, collection_hashes

    db1_hashes, db1_collections = get_db_hashes_and_collections(db1, state)
    db2_hashes, db2_collections = get_db_hashes_and_collections(db2, state)

    print("Comparing database hashes...")
    mismatched_dbs = []
    for db_name in db1_hashes:
        if db_name not in db2_hashes:
            print(db_name)
            mismatched_dbs.append((db_name, "missing in dst DB"))
            print(f"Database '{db_name}' exists in source_DB but not in destination_DB")
        elif db1_hashes[db_name] != db2_hashes[db_name]:
            print(db_name)
            mismatched_dbs.append((db_name, "hash mismatch"))
            print(f"Database '{db_name}' hash mismatch: {db1_hashes[db_name]} != {db2_hashes[db_name]}")

    for db_name in db2_hashes:
        if db_name not in db1_hashes:
            print(db_name)
            mismatched_dbs.append((db_name, "missing in src DB"))
            print(f"Database '{db_name}' exists in destination_DB but not in source_DB")

    print("Comparing collection hashes...")
    mismatched_collections = []
    for coll_name in db1_collections:
        if coll_name not in db2_collections:
            mismatched_collections.append((coll_name, "missing in dst DB"))
            print(f"Collection '{coll_name}' exists in source_DB but not in destination_DB")
        elif db1_collections[coll_name] != db2_collections[coll_name]:
            mismatched_collections.append((coll_name, "hash mismatch"))
            print(f"Collection '{coll_name}' hash mismatch: {db1_collections[coll_name]} != {db2_collections[coll_name]}")

    for coll_name in db2_collections:
        if coll_name not in db1_collections:
            mismatched_collections.append((coll_name, "missing in src DB"))
            print(f"Collection '{coll_name}' exists in destination_DB but not in source_DB")

    return db1_collections.keys() | db2_collections.keys(), mismatched_dbs, mismatched_collections

def compare_entries_number(db1, db2, port, state):
    query = (
        'db.getMongo().getDBNames().forEach(function(i) { '
        '  if (!["admin", "local", "config", "percona_clustersync_mongodb"].includes(i)) { '
        '    var collections = db.getSiblingDB(i).runCommand({ listCollections: 1 }).cursor.firstBatch '
        '      .filter(function(coll) { '
        '        return (!coll.type || coll.type !== "view") && coll.name !== "system.profile"; '
        '      }) '
        '      .map(function(coll) { return coll.name; }); '
        '    collections.forEach(function(coll) { '
        '      if (!(i === "test" && coll === "system.profile")) { '
        '        try { '
        '          var count = db.getSiblingDB(i).getCollection(coll).countDocuments({}); '
        '          print(JSON.stringify({ db: i, collection: coll, count: count })); '
        '        } catch (err) {} '
        '      } '
        '    }); '
        '  } '
        '});'
    )

    def get_collection_counts(db, state):

        if state != "sharded":
            response = db.check_output("mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval '" + query + "' --quiet")
        else:
            response = db.check_output(
                "mongo mongodb://127.0.0.1:27018/test --eval '" + query + "' --quiet")

        collection_counts = {}

        for line in response.split("\n"):
            try:
                count_info = json.loads(line)
                collection_name = f"{count_info['db']}.{count_info['collection']}"
                collection_counts[collection_name] = count_info["count"]
            except json.JSONDecodeError:
                print(f"Warning: Skipping invalid JSON line: {line}")

        return collection_counts

    db1_counts = get_collection_counts(db1,state)
    db2_counts = get_collection_counts(db2, state)

    print("Comparing collection record counts...")
    mismatched_dbs = []
    mismatched_collections = []

    for coll_name in db1_counts:
        if coll_name not in db2_counts:
            mismatched_collections.append((coll_name, "missing in dst DB"))
            print(f"Collection '{coll_name}' exists in source_DB but not in destination_DB")
        elif db1_counts[coll_name] != db2_counts[coll_name]:
            mismatched_collections.append((coll_name, "record count mismatch"))
            print(f"Collection '{coll_name}' record count mismatch: {db1_counts[coll_name]} != {db2_counts[coll_name]}")

    for coll_name in db2_counts:
        if coll_name not in db1_counts:
            mismatched_collections.append((coll_name, "missing in src DB"))
            print(f"Collection '{coll_name}' exists in destination_DB but not in source_DB")

    return db1_counts.keys() | db2_counts.keys(), mismatched_dbs, mismatched_collections

def compare_collection_metadata(db1, db2, port, state):
    print("Comparing collection metadata...")
    mismatched_metadata = []

    db1_metadata = get_all_collection_metadata(db1, port, state)
    db2_metadata = get_all_collection_metadata(db2, port, state)

    db1_collections = {f"{coll['db']}.{coll['name']}": coll for coll in db1_metadata}
    db2_collections = {f"{coll['db']}.{coll['name']}": coll for coll in db2_metadata}

    all_collections = set(db1_collections.keys()).union(set(db2_collections.keys()))

    for coll_name in all_collections:
        if coll_name not in db2_collections:
            mismatched_metadata.append((coll_name, "missing in dst DB"))
            print(f"Collection '{coll_name}' exists in source DB but not in destination DB")
            continue

        if coll_name not in db1_collections:
            mismatched_metadata.append((coll_name, "missing in src DB"))
            print(f"Collection '{coll_name}' exists in destination DB but not in source DB")
            continue

        for field in ["type", "options", "idIndex"]:
            if db1_collections[coll_name].get(field) != db2_collections[coll_name].get(field):
                mismatched_metadata.append((coll_name, f"{field} mismatch"))
                print(f"Collection '{coll_name}' has different {field} in source and destination.")
                print(f"Source DB: {json.dumps(db1_collections[coll_name].get(field), indent=2)}")
                print(f"Destination DB: {json.dumps(db2_collections[coll_name].get(field), indent=2)}")

    return mismatched_metadata

def get_all_collection_metadata(db, port, state):
    query = (
        'db.getMongo().getDBNames().forEach(function(dbName) { '
        '    if (!["admin", "local", "config", "percona_clustersync_mongodb"].includes(dbName)) { '
        '        var collections = db.getSiblingDB(dbName).runCommand({ listCollections: 1 }).cursor.firstBatch '
        '            .filter(function(coll) { '
        '                return !(dbName === "test" && coll.name === "system.profile"); '
        '            }) '
        '            .map(function(coll) { '
        '                return { db: dbName, name: coll.name, type: coll.type, options: coll.options }; '
        '            }); '
        '        print(JSON.stringify(collections)); '
        '    } '
        '});'
    )

    if state != "sharded":
        response = db.check_output(
            "mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --eval '" + query + "' --quiet")
    else:
        response = db.check_output(
            "mongo mongodb://127.0.0.1:27018/test --eval '" + query + "' --quiet")

    try:
        metadata_list = []
        for line in response.splitlines():
            metadata_list.extend(json.loads(line))
        return metadata_list
    except json.JSONDecodeError:
        print("Error: Unable to parse JSON collection metadata response")
        print(f"Raw response: {response}")
        return []

def compare_collection_indexes(db1, db2, all_collections, port, state):
    print("Comparing collection indexes...")
    mismatched_indexes = []

    for coll_name in all_collections:
        db1_indexes = get_indexes(db1, coll_name, port, state)
        db2_indexes = get_indexes(db2, coll_name, port, state)

        db1_index_dict = {index["name"]: index for index in db1_indexes if "name" in index}
        db2_index_dict = {index["name"]: index for index in db2_indexes if "name" in index}

        for index_name, index_details in db1_index_dict.items():
            if index_name not in db2_index_dict:
                mismatched_indexes.append((coll_name, index_name))
                print(f"Collection '{coll_name}': Index '{index_name}' exists in source_DB but not in destination_DB")

        for index_name in db2_index_dict.keys():
            if index_name not in db1_index_dict:
                mismatched_indexes.append((coll_name, index_name))
                print(f"Collection '{coll_name}': Index '{index_name}' exists in destination_DB but not in source_DB")

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
                print(f"Collection '{coll_name}': Index '{index_name}' differs in structure.")
                print(f"Source_DB: {json.dumps(index1_filtered, indent=2)}")
                print(f"Destination_DB: {json.dumps(index2_filtered, indent=2)}")

    return mismatched_indexes

def get_indexes(db, collection_name, port, state):
    db_name, coll_name = collection_name.split(".", 1)

    query = f'db.getSiblingDB("{db_name}").getCollection("{coll_name}").getIndexes()'
    if state != "sharded":
        response = db.check_output(
            "mongo mongodb://127.0.0.1:" + port + "/test?replicaSet=rs --json --eval '" + query + "' --quiet")
    else:
        response = db.check_output(
            "mongo mongodb://127.0.0.1:27018/test --json --eval '" + query + "' --quiet")

    try:
        indexes = json.loads(response)

        def normalize_key(index_key):
            if isinstance(index_key, dict):
                return {k: normalize_key(v) for k, v in index_key.items()}
            elif isinstance(index_key, list):
                return [normalize_key(v) for v in index_key]
            elif isinstance(index_key, dict) and "$numberInt" in index_key:
                return int(index_key["$numberInt"])
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
        print(f"Error: Unable to parse JSON index response for {collection_name}")
        print(f"Raw response: {response}")
        return []