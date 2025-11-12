import pytest
import pymongo
import time

from data_integrity_check import compare_data

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_PML_T50(start_cluster, src_cluster, dst_cluster, csync):
    """Test for array slicing, reversing, filtering, extending, pull, push, concat+slice, nested array updates"""
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    is_sharded = src_cluster.layout == "sharded"
    db, coll = "pipeline_test_db", "array_operations"
    if is_sharded:
        try:
            src.admin.command("enableSharding", db)
        except pymongo.errors.OperationFailure:
            pass
    collection = src[db][coll]
    docs = [
        {"_id": 1, "email": "old@test.com", "phone": "123", "arr": list(range(10))},
        {"_id": 2, "arr": list("ABCDEFG")},
        {"_id": 3, "arr": [1, 2, 3]},
        {"_id": 4, "letters": ["A", "B", "C"]},
        {"_id": 5, "nested": {"arr": list(range(10))}},
        {"_id": 6, "a": ["A", "B", "C", "D", "E", "F", "G", "H"], "b": {"0": ["val1", "val2", "val3"]}},
        {"_id": 7, "arr": ["A", "B", "C", "D", "E", "F"]},
        {"_id": 8, "arr": list("ABCDEFG")},
        {"_id": 9, "arr": list("ABCDEFG")},
        {"_id": 10, "arr": list("ABCDEFG")},
        {"_id": 11, "arr": ["A","B","C","D","E"]},
        {"_id": 12, "arr": ["A", "B", "C", "D"]},
        {"_id": 13, "arr": ["A", "B", "C"]},
        {"_id": 14, "nums": [1, 2, 3]}]
    collection.insert_many(docs)
    sharded_collection = None
    if is_sharded:
        sharded_coll = "sharded_array_operations"
        sharded_collection = src[db][sharded_coll]
        try:
            src.admin.command("shardCollection", f"{db}.{sharded_coll}", key={"_id": "hashed"})
        except pymongo.errors.OperationFailure:
            pass
        sharded_collection.insert_many(docs)
    assert csync.start() and csync.wait_for_repl_stage()
    collections_to_update = [collection]
    if is_sharded and sharded_collection is not None:
        collections_to_update.append(sharded_collection)
    for coll_obj in collections_to_update:
        coll_obj.update_one({"_id": 1}, [
            {"$set": {"email": "new@test.com", "arr": {"$slice": ["$arr", 5]}}},{"$unset": "phone"}])
        coll_obj.update_one({"_id": 2}, [
            {"$set": {"arr": {"$slice": [{"$reverseArray": "$arr"}, 3]}}}])
        coll_obj.update_one({"_id": 3}, [
            {"$set": {"arr": {"$concatArrays": ["$arr", [4, 5]]}}}])
        coll_obj.update_one({"_id": 4}, [
            {"$set": {"letters": {"$filter": {"input": "$letters", "as": "ch", "cond": {"$ne": ["$$ch", "B"]}}}}}])
        coll_obj.update_one({"_id": 5}, [
            {"$set": {"nested.arr": {"$slice": ["$nested.arr", 5]}}}])
        coll_obj.update_one({"_id": 6}, [
            {"$set": {
                "a": {"$filter": {"input": "$a", "as": "val", "cond": {"$ne": ["$$val", "C"]}}}
            }},
            {"$set": {
                "b.0": {"$concatArrays": [
                    {"$slice": ["$b.0", 1]}, ["changed2"], {"$slice": ["$b.0", 2, {"$size": "$b.0"}]}
                ]}
            }},
            {"$set": {
                "a": {"$filter": {"input": "$a", "as": "val", "cond": {"$ne": ["$$val", "F"]}}}}}])
        coll_obj.update_one({"_id": 7}, {"$pull": {"arr": "C"}})
        coll_obj.update_one({"_id": 7}, {"$push": {"arr": {"$each": ["G", "H"], "$slice": 4}}})
        coll_obj.update_one({"_id": 8}, [{"$set": {"arr": {"$slice": ["$arr", 3]}}}])
        coll_obj.update_one({"_id": 9}, [{"$set": {"arr": {"$slice": ["$arr", 1, 4]}}}])
        coll_obj.update_one({"_id": 10}, [{"$set": {"arr": {"$slice": ["$arr", -3]}}}])
        coll_obj.update_one({"_id": 11}, {"$push": {"arr": {"$each": [], "$slice": 3}}})
        coll_obj.update_one({"_id": 12}, {"$pull": {"arr": {"$in": ["C", "D"]}}})
        coll_obj.update_one({"_id": 13},{"$push": {"arr": {"$each": ["X", "Y"], "$position": 1, "$slice": 4}}})
        coll_obj.update_one({"_id": 14},[{"$set": {"doubled": {"$map": {"input": "$nums", "as": "num", "in": {"$multiply": ["$$num", 2]}}}}}])
    assert csync.wait_for_zero_lag() and csync.finalize()
    time.sleep(1)
    collections_to_check = [coll]
    if is_sharded and sharded_collection is not None:
        collections_to_check.append("sharded_array_operations")
    for coll_name in collections_to_check:
        dst_coll = dst[db][coll_name]
        assert dst_coll.find_one({"_id": 1}) == {"_id": 1, "email": "new@test.com", "arr": list(range(5))}
        assert dst_coll.find_one({"_id": 2})["arr"] == ["G", "F", "E"]
        assert dst_coll.find_one({"_id": 3})["arr"] == [1, 2, 3, 4, 5]
        assert dst_coll.find_one({"_id": 4})["letters"] == ["A", "C"]
        assert dst_coll.find_one({"_id": 5})["nested"]["arr"] == [0, 1, 2, 3, 4]
        assert dst_coll.find_one({"_id": 6})["a"] == ["A", "B", "D", "E", "G", "H"]
        assert dst_coll.find_one({"_id": 6})["b"]["0"] == ["val1", "changed2", "val3"]
        assert dst_coll.find_one({"_id": 7})["arr"] == ["A", "B", "D", "E"]
        assert dst_coll.find_one({"_id": 8})["arr"] == ["A", "B", "C"]
        assert dst_coll.find_one({"_id": 9})["arr"] == ["B", "C", "D", "E"]
        assert dst_coll.find_one({"_id": 10})["arr"] == ["E", "F", "G"]
        assert dst_coll.find_one({"_id": 11})["arr"] == ["A", "B", "C"]
        assert dst_coll.find_one({"_id": 12})["arr"] == ["A", "B"]
        assert dst_coll.find_one({"_id": 13})["arr"] == ["A", "X", "Y", "B"]
        assert dst_coll.find_one({"_id": 14})["doubled"] == [2, 4, 6]
    assert compare_data(src_cluster, dst_cluster)[0]
    assert csync.check_csync_errors()[0]

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_PML_T51(start_cluster, src_cluster, dst_cluster, csync):
    """Test for nested path updates, replaceRoot, array mutations, reduce, objectToArray"""
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    is_sharded = src_cluster.layout == "sharded"
    db, coll = "pipeline_test_db", "structural_changes"
    if is_sharded:
        try:
            src.admin.command("enableSharding", db)
        except pymongo.errors.OperationFailure:
            pass
    collection = src[db][coll]
    docs = [
        {"_id": 1, "wrap": {"x": 1, "y": "z"}, "meta": "drop"},
        {"_id": 2, "a": {"0": {"0": "original"}}},
        {
            "_id": 3,
            "i": 1,
            "j": 1,
            "a1": ["A", "B", "C", "D", "E"],
            "a2": [1, 2, 3, 4, 5],
            "f2": {"0": [{"i": x, "0": x} for x in range(5)], "1": "val"}
        },
        {"_id": 4, "a": {"0": "val"}},
        {"_id": 5, "a": {"0": "val"}},
        {"_id": 6, "a": ["val"]},
        {"_id": 7, "a": {"0": ["val"]}},
        {"_id": 8, "a": [{"0": "val"}]},
        {"_id": 9, "a": {"0": ["val"]}},
        {"_id": 10},
        {"_id": 11},
        {"_id": 12, "f2": {"0": [{"i": i, "0": i} for i in range(5)], "1": "val"}},
        {"_id": 13, "nums": [10, 20, 30]},
        {"_id": 14, "obj": {"a": 1, "b": 2}}]
    collection.insert_many(docs)
    sharded_collection = None
    if is_sharded:
        sharded_coll = "sharded_structural_changes"
        sharded_collection = src[db][sharded_coll]
        try:
            src.admin.command("shardCollection", f"{db}.{sharded_coll}", key={"_id": "hashed"})
        except pymongo.errors.OperationFailure:
            pass
        sharded_collection.insert_many(docs)
    assert csync.start() and csync.wait_for_repl_stage()
    collections_to_update = [collection]
    if is_sharded and sharded_collection is not None:
        collections_to_update.append(sharded_collection)
    for coll_obj in collections_to_update:
        coll_obj.update_one({"_id": 1}, [{"$replaceRoot": {"newRoot": "$wrap"}}])
        coll_obj.update_one({"_id": 2}, [{"$set": {"a.0.0": "changed"}}])
        coll_obj.update_one({"_id": 3}, {
            "$inc": {"i": 99},
            "$set": {"field_1": "value_1"},
            "$unset": {"j": 1}})
        coll_obj.update_one({"_id": 3}, {"$set": {"f2.1": "new-val"}})
        coll_obj.update_one({"_id": 3}, {"$set": {"a1.1": "X"}})
        coll_obj.update_one({"_id": 3}, {"$set": {"f2.0.3.0": 99}})
        coll_obj.update_one({"_id": 3}, [{"$set": {"f2.0": {"$concatArrays": ["$f2.0", [{"i": 5, "0": 5}]]}}}])
        coll_obj.update_one({"_id": 3}, [{"$set": {"a2": {"$reverseArray": "$a2"}}}])
        updates = [
            (4, {"$set": {"a": {"0": "changed"}}}),
            (5, {"$set": {"a.0": "changed"}}),
            (6, {"$set": {"a.0": "changed"}}),
            (7, {"$set": {"a.0.0": "changed"}}),
            (8, {"$set": {"a.0.0": "changed"}}),
            (9, {"$set": {"a": {"0.0": "changed"}}}),
            (10, {"$set": {"a.0": {"0.0": "changed"}}})]
        for _id, update in updates:
            coll_obj.update_one({"_id": _id}, update)
        coll_obj.update_one({"_id": 11}, {"$set": {"a.99": "sparse"}})
        coll_obj.update_one({"_id": 12}, [
            {"$set": {
                "f2.0": {
                    "$filter": {"input": "$f2.0", "as": "f", "cond": {"$ne": ["$$f.i", 2]}}}}},
            {"$set": {
                "f2.0": {
                    "$slice": ["$f2.0", -3]}}}])
        coll_obj.update_one({"_id": 13}, [{
            "$set": {
                "sum": {
                    "$reduce": {
                        "input": "$nums",
                        "initialValue": 0,
                        "in": {"$add": ["$$value", "$$this"]}}}}}])
        coll_obj.update_one({"_id": 14}, [
            {"$set": {
                "arr_form": {"$objectToArray": "$obj"}}},
            {"$set": {
                "reconstructed": {"$arrayToObject": "$arr_form"}}}])
    assert csync.wait_for_zero_lag() and csync.finalize()
    time.sleep(1)
    collections_to_check = [coll]
    if is_sharded and sharded_collection is not None:
        collections_to_check.append("sharded_structural_changes")
    for coll_name in collections_to_check:
        dst_coll = dst[db][coll_name]
        assert dst_coll.find_one({"_id": 1}) == {"_id": 1, "x": 1, "y": "z"}
        assert dst_coll.find_one({"_id": 2})["a"]["0"]["0"] == "changed"
        doc = dst_coll.find_one({"_id": 3})
        assert doc["i"] == 100
        assert "j" not in doc
        assert doc["field_1"] == "value_1"
        assert doc["f2"]["1"] == "new-val"
        assert doc["a1"][1] == "X"
        assert doc["f2"]["0"][3]["0"] == 99
        assert doc["f2"]["0"][-1] == {"i": 5, "0": 5}
        assert doc["a2"] == [5, 4, 3, 2, 1]
        assert dst_coll.find_one({"_id": 4})["a"]["0"] == "changed"
        assert dst_coll.find_one({"_id": 5})["a"]["0"] == "changed"
        assert dst_coll.find_one({"_id": 6})["a"][0] == "changed"
        assert dst_coll.find_one({"_id": 7})["a"]["0"][0] == "changed"
        assert dst_coll.find_one({"_id": 8})["a"][0]["0"] == "changed"
        assert dst_coll.find_one({"_id": 9})["a"]["0.0"] == "changed"
        assert dst_coll.find_one({"_id": 10})["a"]["0"]["0.0"] == "changed"
        assert dst_coll.find_one({"_id": 11})["a"]["99"] == "sparse"
        assert len(dst_coll.find_one({"_id": 12})["f2"]["0"]) == 3
        assert dst_coll.find_one({"_id": 13})["sum"] == 60
        doc = dst_coll.find_one({"_id": 14})
        assert isinstance(doc["arr_form"], list)
        assert doc["reconstructed"] == {"a": 1, "b": 2}
    assert compare_data(src_cluster, dst_cluster)[0]
    assert csync.check_csync_errors()[0]

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_PML_T52(start_cluster, src_cluster, dst_cluster, csync):
    """Test for $mergeObjects, $replaceWith, $let, $type, $switch, $cond"""
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    is_sharded = src_cluster.layout == "sharded"
    db, coll = "pipeline_test_db", "extra_operations"
    if is_sharded:
        try:
            src.admin.command("enableSharding", db)
        except pymongo.errors.OperationFailure:
            pass
    collection = src[db][coll]
    docs = [
        {"_id": 1, "nested": {"x": 1}, "score": 93},
        {"_id": 2, "doc": {"foo": "bar"}},
        {"_id": 3},
        {"_id": 4},
        {"_id": 5, "f": 1},
        {"_id": 6,
         "defaults": {"a": 1, "b": 2},
         "overrides": {"a": 100, "c": 3}},
        {"_id": 7, "score": 85},
        {"_id": 8, "score": 72},
        {"_id": 9},
        {"_id": 10, "config": {"nested": {"key": "val"}}}]
    collection.insert_many(docs)
    sharded_collection = None
    if is_sharded:
        sharded_coll = "sharded_extra_operations"
        sharded_collection = src[db][sharded_coll]
        try:
            src.admin.command("shardCollection", f"{db}.{sharded_coll}", key={"_id": "hashed"})
        except pymongo.errors.OperationFailure:
            pass
        sharded_collection.insert_many(docs)
    assert csync.start() and csync.wait_for_repl_stage()
    collections_to_update = [collection]
    if is_sharded and sharded_collection is not None:
        collections_to_update.append(sharded_collection)
    for coll_obj in collections_to_update:
        coll_obj.update_one({"_id": 1}, [
            {"$replaceRoot": {"newRoot": {"$mergeObjects": [{"default": True}, "$$ROOT"]}}},
            {"$set": {"grade": {
                "$switch": {
                    "branches": [
                        {"case": {"$gte": ["$score", 90]}, "then": "A"},
                        {"case": {"$gte": ["$score", 80]}, "then": "B"}
                    ],
                    "default": "F"}}}}])
        coll_obj.update_one({"_id": 2}, [{"$replaceWith": "$doc"}])
        coll_obj.update_one({"_id": 3}, [{"$set": {"f": "$$val"}}], let={"val": 123})
        coll_obj.update_one({"_id": 4}, [
            {"$set": {"first": "$$v"}},
            {"$set": {"nested": {"inner": "$$v"}}}], let={"v": "shared-value"})
        coll_obj.update_one({"_id": 5}, [{"$set": {"field_type": {"$type": "$f"}}}])
        coll_obj.update_one({"_id": 6}, [
            {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$defaults", "$overrides"]}}}])
        coll_obj.update_one({"_id": 7}, [
            {"$set": {"grade": {
                "$cond": {
                    "if": {"$gte": ["$score", 90]},
                    "then": "A",
                    "else": "B"}}}}])
        coll_obj.update_one({"_id": 8}, [
            {"$set": {"grade": {
                "$switch": {
                    "branches": [
                        {"case": {"$gte": ["$score", 90]}, "then": "A"},
                        {"case": {"$gte": ["$score", 80]}, "then": "B"}
                    ],
                    "default": "F"}}}}])
        coll_obj.update_one({"_id": 9}, [
            {"$set": {
                "v1": "$$outer",
                "v2": {"nested": "$$outer"}}},
            {"$set": {
                "v3": {
                    "inner": {
                        "deep": "$$outer"}}}}], let={"outer": {"value": 42, "flag": True}})
        coll_obj.update_one({"_id": 10}, [
            {"$set": {
                "quoted": {"$literal": {"$expr": {"$eq": ["$x", 5]}}},
                "escaped": {"$literal": "$config.nested.key"}}}])
    assert csync.wait_for_zero_lag() and csync.finalize()
    time.sleep(1)
    collections_to_check = [coll]
    if is_sharded and sharded_collection is not None:
        collections_to_check.append("sharded_extra_operations")
    for coll_name in collections_to_check:
        dst_coll = dst[db][coll_name]
        assert dst_coll.find_one({"_id": 1})["grade"] == "A"
        assert dst_coll.find_one({"_id": 1})["default"] is True
        assert dst_coll.find_one({"foo": "bar"})
        assert dst_coll.find_one({"_id": 3})["f"] == 123
        doc = dst_coll.find_one({"_id": 4})
        assert doc["first"] == "shared-value"
        assert doc["nested"]["inner"] == "shared-value"
        assert dst_coll.find_one({"_id": 5})["field_type"] == "int"
        assert dst_coll.find_one({"_id": 6}) == {"_id": 6, "a": 100, "b": 2, "c": 3}
        assert dst_coll.find_one({"_id": 7})["grade"] == "B"
        assert dst_coll.find_one({"_id": 8})["grade"] == "F"
        doc2 = dst_coll.find_one({"_id": 9})
        assert doc2["v1"] == {"value": 42, "flag": True}
        assert doc2["v2"] == {"nested": {"value": 42, "flag": True}}
        assert doc2["v3"]["inner"]["deep"] == {"value": 42, "flag": True}
        doc3 = dst_coll.find_one({"_id": 10})
        assert doc3["quoted"] == {"$expr": {"$eq": ["$x", 5]}}
        assert doc3["escaped"] == "$config.nested.key"
    result, summary = compare_data(src_cluster, dst_cluster)
    if not result:
        critical_mismatches = {"record count mismatch", "missing in dst DB", "missing in src DB"}
        has_critical = any(mismatch[1] in critical_mismatches for mismatch in summary)
        if has_critical:
            pytest.fail("Critical mismatch found:\n" + "\n".join(str(m) for m in summary))
    assert csync.check_csync_errors()[0]

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_PML_T53(start_cluster, src_cluster, dst_cluster, csync):
    """Test for $addFields, $project in updateMany and upsert=True with pipeline"""
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    is_sharded = src_cluster.layout == "sharded"
    db, coll = "pipeline_test_db", "misc_pipeline_stages"
    if is_sharded:
        try:
            src.admin.command("enableSharding", db)
        except pymongo.errors.OperationFailure:
            pass
    collection = src[db][coll]
    docs = [
        {"_id": 1, "a": 2, "b": 3},
        {"_id": 2, "a": 5, "b": 7},
        {"_id": 3, "a": 10, "b": 20}]
    collection.insert_many(docs)
    sharded_collection = None
    if is_sharded:
        sharded_coll = "sharded_misc_pipeline_stages"
        sharded_collection = src[db][sharded_coll]
        try:
            src.admin.command("shardCollection", f"{db}.{sharded_coll}", key={"_id": "hashed"})
        except pymongo.errors.OperationFailure:
            pass
        sharded_collection.insert_many(docs)
    assert csync.start() and csync.wait_for_repl_stage()
    collections_to_update = [collection]
    if is_sharded and sharded_collection is not None:
        collections_to_update.append(sharded_collection)
    for coll_obj in collections_to_update:
        coll_obj.update_many({}, [
            {"$addFields": {"sum": {"$add": ["$a", "$b"]}}},
            {"$project": {"_id": 1, "sum": 1}}])
        coll_obj.update_one(
            {"_id": 99},
            [{"$set": {"note": "inserted via upsert", "ts": "$$NOW"}}],
            upsert=True)
    assert csync.wait_for_zero_lag() and csync.finalize()
    time.sleep(1)
    collections_to_check = [coll]
    if is_sharded and sharded_collection is not None:
        collections_to_check.append("sharded_misc_pipeline_stages")
    for coll_name in collections_to_check:
        dst_coll = dst[db][coll_name]
        for _id, expected_sum in [(1, 5), (2, 12), (3, 30)]:
            doc = dst_coll.find_one({"_id": _id})
            assert doc == {"_id": _id, "sum": expected_sum}
        upserted_doc = dst_coll.find_one({"_id": 99})
        assert upserted_doc["note"] == "inserted via upsert"
        assert "ts" in upserted_doc
    assert compare_data(src_cluster, dst_cluster)[0]
    assert csync.check_csync_errors()[0]

@pytest.mark.parametrize("cluster_configs", ["replicaset", "sharded"], indirect=True)
@pytest.mark.timeout(300, func_only=True)
def test_csync_PML_T54(start_cluster, src_cluster, dst_cluster, csync):
    """Test for documents with '.' and '$' in field names."""
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    is_sharded = src_cluster.layout == "sharded"
    db, coll = "pipeline_test_db", "special_char_fields"
    if is_sharded:
        try:
            src.admin.command("enableSharding", db)
        except pymongo.errors.OperationFailure:
            pass
    collection = src[db][coll]
    doc = {
        "_id": 1,
        "field.with.dot": "initial_dot",
        "field$with$dollar": "initial_dollar"}
    collection.insert_one(doc)
    sharded_collection = None
    if is_sharded:
        sharded_coll = "sharded_special_char_fields"
        sharded_collection = src[db][sharded_coll]
        try:
            src.admin.command("shardCollection", f"{db}.{sharded_coll}", key={"_id": "hashed"})
        except pymongo.errors.OperationFailure:
            pass
        sharded_collection.insert_one(doc)
    assert csync.start() and csync.wait_for_repl_stage()
    collections_to_update = [collection]
    if is_sharded and sharded_collection is not None:
        collections_to_update.append(sharded_collection)
    for coll_obj in collections_to_update:
        coll_obj.update_one({"_id": 1},{"$set": {"field.with.dot": "updated_dot","field$with$dollar": "updated_dollar"}})
    assert csync.wait_for_zero_lag() and csync.finalize()
    time.sleep(1)
    collections_to_check = [coll]
    if is_sharded and sharded_collection is not None:
        collections_to_check.append("sharded_special_char_fields")
    for coll_name in collections_to_check:
        dst_coll = dst[db][coll_name]
        doc_dst = dst_coll.find_one({"_id": 1})
        try:
            assert doc_dst is not None
            assert doc_dst.get("field.with.dot") == "updated_dot"
            assert doc_dst.get("field$with$dollar") == "updated_dollar"
        except AssertionError:
            pytest.xfail("Known limitation: PCSM-139")
    try:
        assert compare_data(src_cluster, dst_cluster)[0]
    except AssertionError:
        pytest.xfail("Known limitation: PCSM-139")
    assert csync.check_csync_errors()[0]