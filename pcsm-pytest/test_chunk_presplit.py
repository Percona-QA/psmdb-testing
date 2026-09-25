import pymongo
import pytest
from bson.max_key import MaxKey
from bson.min_key import MinKey
from cluster import Cluster
from data_integrity_check import compare_data
from pymongo.errors import OperationFailure

# Int64 hash space of a hashed shard key; HASH_MAX is the exclusive upper end.
HASH_MIN = -(2 ** 63)
HASH_MAX = 2 ** 63

def chunks_for_ns(client, ns):
    """Chunks of ns by lower bound."""
    entry = client["config"]["collections"].find_one({"_id": ns})
    assert entry is not None, f"{ns} is missing from config.collections"
    return list(client["config"]["chunks"].find({"uuid": entry["uuid"]}).sort("min", 1))

def chunks_by_shard(chunks):
    """Number of chunks owned by each shard."""
    per_shard = {}
    for chunk in chunks:
        per_shard[chunk["shard"]] = per_shard.get(chunk["shard"], 0) + 1
    return per_shard

def _bound_key(bound):
    """
    Hashable chunk bound that keeps shard-key field order.
    """
    return tuple((field, repr(value)) for field, value in bound.items())

def bounds_to_shard(chunks):
    """Map each chunk's (min, max) bounds to the shard owning it."""
    return {(_bound_key(chunk["min"]), _bound_key(chunk["max"])): chunk["shard"]
            for chunk in chunks}

def shard_hash_widths(chunks, field):
    """Total hash-space width owned by each shard, for a hashed shard key."""
    widths = {}
    for chunk in chunks:
        low = chunk["min"][field]
        high = chunk["max"][field]
        low = HASH_MIN if isinstance(low, MinKey) else int(low)
        high = HASH_MAX if isinstance(high, MaxKey) else int(high)
        widths[chunk["shard"]] = widths.get(chunk["shard"], 0) + (high - low)
    return widths

def sorted_shard_ids(client):
    """Shard IDs as PCSM sees them: listShards output, sorted."""
    return sorted(shard["_id"] for shard in client.admin.command("listShards")["shards"])

def shard_pairing(src, dst):
    """Source to target shard mapping PCSM uses when shard counts are equal."""
    return dict(zip(sorted_shard_ids(src), sorted_shard_ids(dst), strict=True))

def database_primary(client, db_name):
    entry = client["config"]["databases"].find_one({"_id": db_name})
    assert entry is not None, f"database {db_name} is missing from config.databases"
    return entry["primary"]

def docs_per_shard(cluster, db_name, coll_names):
    """
    Documents physically stored on each shard primary.
    """
    counts = {}
    for shard_id, client in cluster.get_shard_primary_clients():
        try:
            counts[shard_id] = sum(client[db_name][coll].count_documents({})
                                   for coll in coll_names)
        finally:
            client.close()
    return counts

def balancer_state(client):
    """Balancer mode plus the config.settings document PCSM must not touch."""
    return {
        "mode": client.admin.command("balancerStatus").get("mode"),
        "settings": client["config"]["settings"].find_one({"_id": "balancer"}),
    }

def pin_source_layout(client, ns):
    """Keep the source layout stable while PCSM reads it. PCSM ignores noBalance."""
    client["config"]["collections"].update_one({"_id": ns}, {"$set": {"noBalance": True}})

def move_chunk(client, ns, chunk, to_shard):
    """Move a chunk by its exact bounds."""
    try:
        client.admin.command("moveChunk", ns, bounds=[chunk["min"], chunk["max"]], to=to_shard)
    except OperationFailure as e:
        if "not supported" in str(e).lower() or "command not found" in str(e).lower():
            pytest.skip(f"moveChunk not supported: {e}")
        raise

def scatter_chunks(client, ns, shard_ids):
    """Give every shard a chunk, so the source layout spans all of them."""
    chunks = chunks_for_ns(client, ns)
    primary = chunks[0]["shard"]
    non_primary = [shard for shard in shard_ids if shard != primary]
    assert len(chunks) > len(non_primary), (
        f"{ns}: need more chunks than non-primary shards to scatter, "
        f"got {len(chunks)} chunks and {len(non_primary)} non-primary shards")
    # Take chunks from the end so the first chunk stays on the primary shard.
    for shard, chunk in zip(non_primary, chunks[1:], strict=False):
        move_chunk(client, ns, chunk, shard)

def assert_source_spans_shards(client, ns, shard_ids):
    """
    Check that scatter_chunks worked before the sync starts.
    """
    owners = set(chunks_by_shard(chunks_for_ns(client, ns)))
    assert owners == set(shard_ids), (
        f"{ns}: source layout is expected to span every shard, "
        f"owners={owners}, shards={shard_ids}")

def assert_mirrored(src, dst, ns, pairing):
    """Target has the source's chunk boundaries, owned by the paired shards."""
    src_chunks = chunks_for_ns(src, ns)
    tgt_chunks = chunks_for_ns(dst, ns)
    Cluster.log(f"{ns} source layout: {chunks_by_shard(src_chunks)}")
    Cluster.log(f"{ns} target layout: {chunks_by_shard(tgt_chunks)}")
    assert len(tgt_chunks) == len(src_chunks), (
        f"{ns}: target has {len(tgt_chunks)} chunks, source has {len(src_chunks)}")
    src_owner = bounds_to_shard(src_chunks)
    tgt_owner = bounds_to_shard(tgt_chunks)
    assert src_owner.keys() == tgt_owner.keys(), (
        f"{ns}: target chunk boundaries differ from the source\n"
        f"source: {sorted(src_owner.keys())}\ntarget: {sorted(tgt_owner.keys())}")
    for bounds, src_shard in src_owner.items():
        assert tgt_owner[bounds] == pairing[src_shard], (
            f"{ns}: chunk {bounds} is on {tgt_owner[bounds]}, expected "
            f"{pairing[src_shard]} (paired with source shard {src_shard})")

def assert_no_unexpected_errors(csync):
    """
    Fail on error lines other than transient retry warnings.
    """
    csync_error, error_logs = csync.check_csync_errors()
    if csync_error:
        return
    unexpected = [line for line in error_logs if "Transient error" not in line]
    assert not unexpected, f"Csync reported errors in logs: {unexpected}"

def configure_move_chunk_failpoint(cluster, mode, error_code):
    """
    Configure the failpoint on mongos to fail `moveChunk` with `error_code`.
    """
    client = pymongo.MongoClient(cluster.connection)
    try:
        client.admin.command("configureFailPoint", "failCommand", mode=mode,
                             data={"failCommands": ["moveChunk"],
                                   "errorCode": error_code})
    except OperationFailure as e:
        Cluster.log(f"failCommand is not configurable on mongos: {e}")
        return False
    finally:
        client.close()
    Cluster.log(f"moveChunk failpoint '{mode}' (code {error_code}) configured on mongos")
    return True

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_pcsm_presplit_ranged_mirrors_source_PML_T121(start_cluster, src_cluster,
                                                      dst_cluster, csync):
    """
    Two ranged collections:
    - shard counts are equal, so the split collection is mirrored: same
      boundaries, each chunk on the target shard paired with its source owner
      by sorted shard ID;
    - the unsplit collection has no boundary to replay, so pre-split does
      nothing and its single chunk stays on the target primary shard.
    Neither balancer may change, checked after the clone and after finalize.
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "presplit_mirror_db"
    coll_name = "ranged_coll"
    ns = f"{db_name}.{coll_name}"
    unsplit_ns = f"{db_name}.unsplit_coll"
    src_shards = sorted_shard_ids(src)
    assert len(src_shards) == len(sorted_shard_ids(dst)), \
        "mirroring requires equal source and target shard counts"
    src_balancer_before = balancer_state(src)
    dst_balancer_before = balancer_state(dst)
    Cluster.log(f"balancer before: source={src_balancer_before}, target={dst_balancer_before}")
    src.admin.command("enableSharding", db_name)
    src.admin.command("shardCollection", ns, key={"_id": 1})
    pin_source_layout(src, ns)
    for point in (0, 100, 200):
        src.admin.command("split", ns, middle={"_id": point})
    scatter_chunks(src, ns, src_shards)
    src[db_name][coll_name].insert_many(
        [{"_id": i, "value": f"v_{i}"} for i in range(-50, 300, 5)])
    assert_source_spans_shards(src, ns, src_shards)
    src.admin.command("shardCollection", unsplit_ns, key={"_id": 1})
    pin_source_layout(src, unsplit_ns)
    src[db_name]["unsplit_coll"].insert_many(
        [{"_id": i, "value": f"v_{i}"} for i in range(200)])
    unsplit_src_chunks = chunks_for_ns(src, unsplit_ns)
    assert len(unsplit_src_chunks) == 1, \
        f"expected an unsplit source collection, got {chunks_by_shard(unsplit_src_chunks)}"
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to complete the initial clone"
    assert_mirrored(src, dst, ns, shard_pairing(src, dst))
    assert csync.wait_for_log(f"Pre-split ranged collection {ns}: mirrored"), \
        f"PCSM did not log a mirrored pre-split for {ns}"
    unsplit_tgt_chunks = chunks_for_ns(dst, unsplit_ns)
    tgt_primary = database_primary(dst, db_name)
    Cluster.log(f"{unsplit_ns} target layout: {chunks_by_shard(unsplit_tgt_chunks)}, "
                f"primary {tgt_primary}")
    assert len(unsplit_tgt_chunks) == 1, \
        f"target was split despite a single source chunk: " \
        f"{chunks_by_shard(unsplit_tgt_chunks)}"
    assert unsplit_tgt_chunks[0]["shard"] == tgt_primary, \
        (f"single chunk is on {unsplit_tgt_chunks[0]['shard']}, expected primary shard "
         f"{tgt_primary}")
    assert not csync.wait_for_log(f"Pre-split ranged collection {unsplit_ns}", timeout=1), \
        f"PCSM logged a pre-split for single-chunk collection {unsplit_ns}"
    assert balancer_state(src) == src_balancer_before, "source balancer changed during clone"
    assert balancer_state(dst) == dst_balancer_before, "target balancer changed during clone"
    assert csync.finalize(), "Failed to finalize csync service"
    assert balancer_state(src) == src_balancer_before, (
        f"source balancer changed: before={src_balancer_before}, after={balancer_state(src)}")
    assert balancer_state(dst) == dst_balancer_before, (
        f"target balancer changed: before={dst_balancer_before}, after={balancer_state(dst)}")
    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Data mismatch: {summary}"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.timeout(600, func_only=True)
def test_pcsm_presplit_compound_shard_keys_PML_T122(start_cluster, src_cluster,
                                                    dst_cluster, csync):
    """
    Compound shard keys:
    - {a: 1, b: 1} is ranged, so it is mirrored with the field order kept;
    - {a: "hashed"} is not pre-split: the source is skewed onto one shard, but
      the target keeps the even native layout and an even share of the hash
      space. Only evenness is asserted, chunk counts are version dependent;
    - {a: 1, b: "hashed"} has a hashed field, so pre-split is skipped and the
      target keeps its single native chunk.
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "presplit_compound_db"
    ranged_ns = f"{db_name}.coll_ranged"
    hashed_prefix_ns = f"{db_name}.coll_hashed_prefix"
    hashed_suffix_ns = f"{db_name}.coll_hashed_suffix"
    src_shards = sorted_shard_ids(src)
    tgt_shards = sorted_shard_ids(dst)
    assert len(src_shards) == len(tgt_shards), \
        "mirroring requires equal source and target shard counts"
    src.admin.command("enableSharding", db_name)
    src.admin.command("shardCollection", ranged_ns, key={"a": 1, "b": 1})
    pin_source_layout(src, ranged_ns)
    for point in (100, 200):
        src.admin.command("split", ranged_ns, middle={"a": point, "b": 0})
    scatter_chunks(src, ranged_ns, src_shards)
    src[db_name]["coll_ranged"].insert_many(
        [{"_id": i, "a": i, "b": i % 5} for i in range(300)])
    assert_source_spans_shards(src, ranged_ns, src_shards)
    src.admin.command("shardCollection", hashed_prefix_ns, key={"a": "hashed"})
    pin_source_layout(src, hashed_prefix_ns)
    src[db_name]["coll_hashed_prefix"].insert_many(
        [{"_id": i, "a": i} for i in range(200)])
    for chunk in chunks_for_ns(src, hashed_prefix_ns):
        if chunk["shard"] != src_shards[0]:
            move_chunk(src, hashed_prefix_ns, chunk, src_shards[0])
    assert set(chunks_by_shard(chunks_for_ns(src, hashed_prefix_ns))) == {src_shards[0]}, \
        "failed to skew the source hashed layout onto a single shard"
    src.admin.command("shardCollection", hashed_suffix_ns, key={"a": 1, "b": "hashed"})
    pin_source_layout(src, hashed_suffix_ns)
    src[db_name]["coll_hashed_suffix"].insert_many(
        [{"_id": i, "a": i, "b": i % 7} for i in range(200)])
    suffix_chunks = chunks_for_ns(src, hashed_suffix_ns)
    assert len(suffix_chunks) == 1, \
        f"expected one source chunk for a non-hashed prefix, got {len(suffix_chunks)}"
    suffix_destination = next(shard for shard in src_shards
                              if shard != suffix_chunks[0]["shard"])
    move_chunk(src, hashed_suffix_ns, suffix_chunks[0], suffix_destination)
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(), "Failed to complete the initial clone"
    assert_mirrored(src, dst, ranged_ns, shard_pairing(src, dst))
    assert csync.wait_for_log(f"Pre-split ranged collection {ranged_ns}: mirrored"), \
        f"PCSM did not log a mirrored pre-split for {ranged_ns}"
    prefix_chunks = chunks_for_ns(dst, hashed_prefix_ns)
    prefix_per_shard = chunks_by_shard(prefix_chunks)
    Cluster.log(f"{hashed_prefix_ns} target layout: {prefix_per_shard}")
    assert set(prefix_per_shard) == set(tgt_shards), \
        f"hashed prefix layout did not stay native and even: {prefix_per_shard}"
    assert len(set(prefix_per_shard.values())) == 1, \
        f"uneven chunk ownership for a hashed prefix: {prefix_per_shard}"
    widths = shard_hash_widths(prefix_chunks, "a")
    ideal = (HASH_MAX - HASH_MIN) / len(tgt_shards)
    for shard, width in widths.items():
        assert abs(width - ideal) / ideal < 0.05, (
            f"shard {shard} owns {width / (HASH_MAX - HASH_MIN):.1%} of the hash space, "
            f"expected about {1 / len(tgt_shards):.1%}: {widths}")
    suffix_chunks = chunks_for_ns(dst, hashed_suffix_ns)
    suffix_primary = database_primary(dst, db_name)
    Cluster.log(f"{hashed_suffix_ns} target layout: {chunks_by_shard(suffix_chunks)}, "
                f"primary {suffix_primary}")
    assert len(suffix_chunks) == 1, \
        f"hashed suffix collection was pre-split: {chunks_by_shard(suffix_chunks)}"
    assert suffix_chunks[0]["shard"] == suffix_primary, \
        (f"hashed suffix chunk is on {suffix_chunks[0]['shard']}, expected the primary "
         f"shard {suffix_primary}: source ownership must not be mirrored")
    for hashed_ns in (hashed_prefix_ns, hashed_suffix_ns):
        assert not csync.wait_for_log(f"Pre-split ranged collection {hashed_ns}", timeout=1), \
            f"PCSM logged a pre-split for hashed collection {hashed_ns}"
    assert csync.finalize(), "Failed to finalize csync service"
    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Data mismatch: {summary}"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["sharded_3v2"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(900, func_only=True)
def test_pcsm_presplit_ranged_weighted_placement_PML_T123(start_cluster, src_cluster,
                                                          dst_cluster, csync):
    """
    With unequal shard counts the boundaries are replayed and chunks placed
    largest-first onto the lightest shard. Every target shard must own chunks,
    and data volume must stay even to within the heaviest chunk.
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "presplit_weighted_db"
    coll_name = "ranged_coll"
    ns = f"{db_name}.{coll_name}"
    src_shards = sorted_shard_ids(src)
    tgt_shards = sorted_shard_ids(dst)
    assert len(src_shards) != len(tgt_shards), (
        f"size-weighted placement requires unequal shard counts, "
        f"source={src_shards}, target={tgt_shards}")

    src.admin.command("enableSharding", db_name)
    src.admin.command("shardCollection", ns, key={"_id": 1})
    pin_source_layout(src, ns)
    for point in (0, 100, 200, 300, 400):
        src.admin.command("split", ns, middle={"_id": point})
    scatter_chunks(src, ns, src_shards)
    heavy_docs = 100
    docs = [{"_id": i, "value": f"v_{i}"} for i in range(heavy_docs)]
    for start in (-40, 100, 200, 300, 400):
        docs += [{"_id": i, "value": f"v_{i}"} for i in range(start, start + 30)]
    src[db_name][coll_name].insert_many(docs)
    assert_source_spans_shards(src, ns, src_shards)
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(timeout=300), "Failed to complete the initial clone"
    src_chunks = chunks_for_ns(src, ns)
    tgt_chunks = chunks_for_ns(dst, ns)
    per_shard = chunks_by_shard(tgt_chunks)
    Cluster.log(f"{ns} source layout: {chunks_by_shard(src_chunks)}")
    Cluster.log(f"{ns} target layout: {per_shard}")
    assert len(tgt_chunks) == len(src_chunks), \
        f"target has {len(tgt_chunks)} chunks, source has {len(src_chunks)}"
    assert set(per_shard) == set(tgt_shards), \
        f"not every target shard owns chunks: {per_shard}"
    counts = docs_per_shard(dst_cluster, db_name, [coll_name])
    total = sum(counts.values())
    ideal = total / len(tgt_shards)
    Cluster.log(f"{ns} documents per target shard: {counts} (ideal {ideal:.0f})")
    assert total == len(docs), f"target holds {total} documents, expected {len(docs)}"
    assert max(counts.values()) <= ideal + heavy_docs, \
        f"uneven data distribution: {counts} (ideal {ideal:.0f})"
    assert min(counts.values()) >= total * 0.2, \
        f"a target shard holds almost no data: {counts} of {total} documents"
    assert csync.wait_for_log(f"Pre-split ranged collection {ns}: size-weighted"), \
        f"PCSM did not log a size-weighted pre-split for {ns}"
    assert csync.finalize(), "Failed to finalize csync service"
    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Data mismatch: {summary}"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

@pytest.mark.parametrize("cluster_configs", ["sharded_3v2"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(900, func_only=True)
def test_pcsm_presplit_weighting_is_cumulative_PML_T124(start_cluster, src_cluster,
                                                        dst_cluster, csync):
    """
    Chunk sizes accumulate per target shard across the whole run, not per
    collection. Four identical collections have one heavy and one light chunk
    each: balancing them in isolation piles every heavy chunk on one shard
    (~90/10), run-wide accumulation alternates them (~50/50).
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "presplit_cumulative_db"
    coll_names = [f"ranged_coll_{i}" for i in range(4)]
    heavy_docs = 100
    light_docs = 10
    src_shards = sorted_shard_ids(src)
    tgt_shards = sorted_shard_ids(dst)
    assert len(src_shards) != len(tgt_shards), (
        f"size-weighted placement requires unequal shard counts, "
        f"source={src_shards}, target={tgt_shards}")
    src.admin.command("enableSharding", db_name)
    for coll_name in coll_names:
        ns = f"{db_name}.{coll_name}"
        src.admin.command("shardCollection", ns, key={"_id": 1})
        pin_source_layout(src, ns)
        src.admin.command("split", ns, middle={"_id": 0})
        src[db_name][coll_name].insert_many(
            [{"_id": i, "value": f"v_{i}"} for i in range(-light_docs, heavy_docs)])
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(timeout=300), "Failed to complete the initial clone"
    owners = set()
    for coll_name in coll_names:
        tgt_chunks = chunks_for_ns(dst, f"{db_name}.{coll_name}")
        Cluster.log(f"{db_name}.{coll_name} target layout: {chunks_by_shard(tgt_chunks)}")
        assert len(tgt_chunks) == 2, \
            f"{coll_name}: target has {len(tgt_chunks)} chunks, expected 2"
        owners.update(chunk["shard"] for chunk in tgt_chunks)
        assert csync.wait_for_log(
            f"Pre-split ranged collection {db_name}.{coll_name}: size-weighted"), \
            f"PCSM did not log a size-weighted pre-split for {coll_name}"
    assert owners == set(tgt_shards), f"not every target shard owns chunks: {owners}"
    counts = docs_per_shard(dst_cluster, db_name, coll_names)
    total = sum(counts.values())
    Cluster.log(f"documents per target shard across {len(coll_names)} collections: {counts}")
    assert total == len(coll_names) * (heavy_docs + light_docs), \
        f"target holds {total} documents, expected {len(coll_names) * (heavy_docs + light_docs)}"
    assert max(counts.values()) <= total * 0.6, (
        f"heavy chunks piled onto one shard, which means sizes were not accumulated "
        f"across collections: {counts} of {total} documents")
    assert csync.finalize(), "Failed to finalize csync service"
    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Data mismatch: {summary}"
    assert_no_unexpected_errors(csync)

@pytest.mark.parametrize("cluster_configs", ["sharded_3v2"], indirect=True)
@pytest.mark.jenkins
@pytest.mark.timeout(900, func_only=True)
def test_pcsm_presplit_empty_chunks_spread_PML_T125(start_cluster, src_cluster,
                                                    dst_cluster, csync):
    """
    Size-weighted placement must spread chunks over every target shard even
    when they hold no data. Ranking by bytes alone sent every zero-size chunk
    to the same shard, so they now rank by chunk count.
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "presplit_empty_chunks_db"
    coll_name = "empty_coll"
    ns = f"{db_name}.{coll_name}"
    src_shards = sorted_shard_ids(src)
    tgt_shards = sorted_shard_ids(dst)
    assert len(src_shards) != len(tgt_shards), (
        f"size-weighted placement requires unequal shard counts, "
        f"source={src_shards}, target={tgt_shards}")
    src.admin.command("enableSharding", db_name)
    src.admin.command("shardCollection", ns, key={"_id": 1})
    pin_source_layout(src, ns)
    for point in (0, 100, 200, 300, 400):
        src.admin.command("split", ns, middle={"_id": point})
    scatter_chunks(src, ns, src_shards)
    assert_source_spans_shards(src, ns, src_shards)
    assert csync.start(), "Failed to start csync"
    assert csync.wait_for_repl_stage(timeout=300), "Failed to complete the initial clone"
    src_chunks = chunks_for_ns(src, ns)
    tgt_chunks = chunks_for_ns(dst, ns)
    per_shard = {shard: 0 for shard in tgt_shards}
    per_shard.update(chunks_by_shard(tgt_chunks))
    Cluster.log(f"{ns} source layout: {chunks_by_shard(src_chunks)}")
    Cluster.log(f"{ns} target layout: {per_shard}")
    assert len(tgt_chunks) == len(src_chunks), \
        f"target has {len(tgt_chunks)} chunks, source has {len(src_chunks)}"
    idle = sorted(shard for shard, count in per_shard.items() if count == 0)
    assert not idle, (
        f"target shards {idle} own no chunk, so every write into those ranges "
        f"after the cutover is routed to the {len(per_shard) - len(idle)} shards "
        f"that do")
    assert max(per_shard.values()) - min(per_shard.values()) <= 1, \
        f"zero-size chunks are not spread evenly over the target shards: {per_shard}"
    assert csync.finalize(), "Failed to finalize csync service"
    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Data mismatch: {summary}"
    assert_no_unexpected_errors(csync)

# Chunk-migration codes which are classified as retryable for moveChunk.
MOVE_RETRY_CODES = {
    "LockTimeout": 24,
    "ExceededTimeLimit": 262,
    "RetriableRemoteCommandFailure": 91331,
    "Interrupted": 11601,
}
@pytest.mark.parametrize("cluster_configs", ["sharded"], indirect=True)
@pytest.mark.parametrize("error_name,error_code", MOVE_RETRY_CODES.items())
@pytest.mark.mongos_extra_args("--setParameter enableTestCommands=1")
@pytest.mark.jenkins
@pytest.mark.timeout(900, func_only=True)
def test_pcsm_presplit_move_is_retried_PML_T126(start_cluster, src_cluster,
                                                dst_cluster, csync,
                                                error_name, error_code):
    """
    PCSM-381: chunk-migration errors are retried, and a move that never
    succeeds does not fail the clone.

    The failpoint forbids chunk moves outright, so no move can land. PCSM must
    retry, then keep the target layout and carry on cloning.
    """
    src = pymongo.MongoClient(src_cluster.connection)
    dst = pymongo.MongoClient(dst_cluster.connection)
    db_name = "presplit_move_failure_db"
    coll_name = "ranged_coll"
    ns = f"{db_name}.{coll_name}"
    src_shards = sorted_shard_ids(src)
    src.admin.command("enableSharding", db_name)
    src.admin.command("shardCollection", ns, key={"_id": 1})
    pin_source_layout(src, ns)
    for point in (0, 100, 200):
        src.admin.command("split", ns, middle={"_id": point})
    scatter_chunks(src, ns, src_shards)
    src[db_name][coll_name].insert_many(
        [{"_id": i, "value": f"v_{i}"} for i in range(-50, 300, 5)])
    assert_source_spans_shards(src, ns, src_shards)
    Cluster.log(f"forbidding every chunk move with {error_name} ({error_code})")
    if not configure_move_chunk_failpoint(dst_cluster, "alwaysOn", error_code=error_code):
        pytest.skip("failCommand failpoint is unavailable on the target mongos")
    try:
        assert csync.start(), "Failed to start csync"
        assert csync.wait_for_log(f"Transient error: ({error_name})", timeout=120), \
            f"the chunk move failing with {error_name} was not retried, " \
            "it was treated as a final failure"
        assert csync.wait_for_log("retry attempt 2", timeout=120), \
            "the chunk move was retried once and then given up on"
        assert csync.wait_for_repl_stage(timeout=300), \
            "the clone did not finish after a chunk-move failure"
    finally:
        configure_move_chunk_failpoint(dst_cluster, "off", error_code)
    status = csync.status().get("data", {})
    Cluster.log(f"status after the chunk-move failures: {status}")
    assert status.get("state") == "running", \
        f"a pre-split move failure left the run in {status.get('state')}: {status.get('error')}"
    assert status.get("initialSync", {}).get("completed") is True, \
        f"initial sync did not complete after a chunk-move failure: {status}"
    Cluster.log(f"{ns} target layout: {chunks_by_shard(chunks_for_ns(dst, ns))}")
    assert csync.wait_for_log(
        f'Pre-split of "{ns}" failed, keeping the native chunk layout'), \
        "a move that exhausted its retries did not fall back to the native layout"
    assert csync.wait_for_log(f'Collection "{ns}" cloned'), \
        "the collection whose pre-split failed was not cloned"
    assert csync.finalize(), "Failed to finalize csync service"
    result, summary = compare_data(src_cluster, dst_cluster)
    assert result is True, f"Data mismatch: {summary}"
    assert_no_unexpected_errors(csync)