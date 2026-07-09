import time

import pytest
import pymongo

from datetime import datetime
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}

@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)

@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.fixture(scope="module")
def oplog_chunks(cluster):
    cluster.destroy()
    cluster.create()
    cluster.setup_pbm()
    cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    cluster.wait_pitr_chunk()
    valid_start = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.wait_pitr_chunk()
    valid_end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(valid_end)
    yield {"start": valid_start, "end": valid_end}
    cluster.destroy(cleanup_backups=True)

@pytest.fixture(scope="function")
def start_sharded_cluster(request):
    sharded_config = {
        "mongos": "mongos",
        "configserver": {"_id": "cfg", "members": [{"host": "cfg01"}]},
        "shards": [
            {"_id": "sh1", "members": [{"host": "sh1rs01"}]},
            {"_id": "sh2", "members": [{"host": "sh2rs01"}]},
        ],
    }
    sharded = Cluster(sharded_config)
    try:
        sharded.destroy()
        sharded.create()
        sharded.setup_pbm()
        yield sharded
    finally:
        if request.config.getoption("--verbose"):
            sharded.get_logs()
        sharded.destroy(cleanup_backups=True)

@pytest.mark.timeout(300, func_only=True)
@pytest.mark.parametrize("command_template, expected_rc, expected_error", [
    ('oplog-replay --end="{end}" --wait', 1, 'required flag(s) "start" not set'),
    ('oplog-replay --start="{start}" --wait', 1, 'required flag(s) "end" not set'),
    ('oplog-replay --start="test" --end="{end}" --wait', 1, 'parse start time'),
    ('oplog-replay --start="{start}" --end="test" --wait', 1, 'parse end time'),
    ('oplog-replay --start="2020-01-01T00:00:00" --end="{end}" --wait', 1, 'integrity'),
    ('oplog-replay --start="{end}" --end="{start}" --wait', 0, None),
    ('oplog-replay --start="2099-01-01T00:00:00" --end="2099-12-31T00:00:00" --wait', 0, None),
])
def test_oplog_replay_validation_PBM_T349(oplog_chunks, cluster, command_template, expected_rc, expected_error):
    """
    Verify pbm oplog-replay validation
    """
    command = command_template.format(**oplog_chunks)
    result = cluster.exec_pbm_cli(command)
    assert result.rc == expected_rc, (
        f"Expected rc={expected_rc}, got rc={result.rc}. stdout={result.stdout} stderr={result.stderr}"
    )
    if expected_error:
        assert expected_error in result.stderr, f"Expected '{expected_error}' in stderr, got: {result.stderr}"

@pytest.mark.timeout(300, func_only=True)
def test_oplog_replay_logical_PBM_T350(start_cluster, cluster):
    """
    Verify oplog replay restores post logical backup data.
    """
    client = pymongo.MongoClient(cluster.connection)
    for i in range(100):
        client["test"]["test"].insert_one({"phase": "pre", "i": i})

    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    cluster.wait_pitr_chunk()
    start = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    for i in range(100):
        client["test"]["test"].insert_one({"phase": "post", "i": i})

    time.sleep(1)
    end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(end)

    assert client["test"]["test"].count_documents({}) == 200

    cluster.make_restore(backup, check_pbm_status=True)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 100

    cluster.oplog_replay(start, end)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 200

    cluster.check_pbm_status()
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300, func_only=True)
def test_oplog_replay_physical_PBM_T351(start_cluster, cluster):
    """
    Verify oplog replay restores post physical backup data.
    """
    client = pymongo.MongoClient(cluster.connection)
    for i in range(100):
        client["test"]["test"].insert_one({"phase": "pre", "i": i})

    backup = cluster.make_backup("physical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    cluster.wait_pitr_chunk()
    start = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    for i in range(100):
        client["test"]["test"].insert_one({"phase": "post", "i": i})

    time.sleep(1)
    end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(end)

    assert client["test"]["test"].count_documents({}) == 200

    cluster.make_restore(backup, restart_cluster=True, check_pbm_status=True)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 100

    cluster.oplog_replay(start, end)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 200

    cluster.check_pbm_status()
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300, func_only=True)
def test_oplog_replay_wait_time_PBM_T352(start_cluster, cluster):
    """
    Verify --wait-time is accepted and the replay completes successfully when
    the operation finishes within the allotted time.
    """
    client = pymongo.MongoClient(cluster.connection)
    for i in range(100):
        client["test"]["test"].insert_one({"phase": "pre", "i": i})

    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    cluster.wait_pitr_chunk()
    start = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    for i in range(100):
        client["test"]["test"].insert_one({"phase": "post", "i": i})

    time.sleep(1)
    end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(end)

    cluster.make_restore(backup, check_pbm_status=True)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 100

    cluster.oplog_replay(start, end, **{"wait-time": "5m"})
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 200

    cluster.check_pbm_status()
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300, func_only=True)
def test_oplog_replay_wait_time_PBM_T353(start_cluster, cluster):  # noqa: E501
    """
    Verify --wait-time is accepted and the replay completes successfully when
    the operation finishes outside the allotted time.
    """
    client = pymongo.MongoClient(cluster.connection)

    for i in range(10):
        client["test"]["test"].create_index([(f"field_{i}", 1)])

    for i in range(1000):
        client["test"]["test"].insert_one({f"field_{j}": i for j in range(10)})

    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    cluster.wait_pitr_chunk()
    start = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    for _ in range(10):
        batch = [{f"field_{j}": i for j in range(10)} for i in range(1000)]
        client["test"]["test"].insert_many(batch)

    time.sleep(1)
    end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(end)

    cluster.make_restore(backup, check_pbm_status=True)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 1000

    result = cluster.oplog_replay(start, end, **{"wait-time": "3s"})
    assert "operation is in progress. check pbm status and logs" in result.stdout.lower(), (
        f"Expected 'operation is in progress. check pbm status and logs', got: {result.stdout}"
    )

    deadline = time.time() + 300
    while cluster.get_status()["running"] and time.time() < deadline:
        time.sleep(5)
    assert not cluster.get_status()["running"], "Oplog replay did not complete within 300s"

    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 11000

    cluster.check_pbm_status()
    Cluster.log("Finished successfully")

@pytest.mark.timeout(300, func_only=True)
def test_oplog_replay_replset_remapping_PBM_T354():
    """
    Verify --replset-remapping successfully completes on a cluster with a different RS name
    """
    source = Cluster({"_id": "rs_source", "members": [{"host": "rs101"}]})
    target = Cluster({"_id": "rs_target", "members": [{"host": "rs101"}]})

    source.destroy()
    source.create()
    source.setup_pbm()

    try:
        client = pymongo.MongoClient(source.connection)
        for i in range(100):
            client["test"]["test"].insert_one({"phase": "pre", "i": i})

        backup = source.make_backup("logical")
        source.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
        source.wait_pitr_chunk()
        start = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

        for i in range(100):
            client["test"]["test"].insert_one({"phase": "post", "i": i})

        time.sleep(1)
        end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        source.disable_pitr(end)
    finally:
        source.destroy()

    try:
        target.create()
        target.setup_pbm()

        target.make_restore(backup, restore_opts=["--replset-remapping rs_target=rs_source"], check_pbm_status=True)
        client = pymongo.MongoClient(target.connection)
        assert client["test"]["test"].count_documents({}) == 100

        # Wrong source name — PBM's topology check detects the mismatch and fails clearly
        result = target.oplog_replay(start, end, allow_fail=True, **{"replset-remapping": "rs_target=rs_nonexistent"})
        assert result.rc != 0, "Expected oplog replay to fail with wrong remapping"
        assert "missed replset" in result.stderr, (f"Expected 'missed replset' in stderr, got: {result.stderr}")
        client = pymongo.MongoClient(target.connection)
        assert client["test"]["test"].count_documents({}) == 100

        # Correct remapping now succeeds
        target.oplog_replay(start, end, **{"replset-remapping": "rs_target=rs_source"})
        client = pymongo.MongoClient(target.connection)
        assert client["test"]["test"].count_documents({}) == 200

        target.check_pbm_status()
        Cluster.log("Finished successfully")
    finally:
        target.destroy(cleanup_backups=True)

@pytest.mark.timeout(300, func_only=True)
def test_oplog_replay_sharded_PBM_T355(start_sharded_cluster):
    """
    Verify oplog replay works on a sharded cluster.
    """
    cluster = start_sharded_cluster
    client = pymongo.MongoClient(cluster.connection)
    client["test"]["test"].insert_many([{"phase": "pre", "i": i} for i in range(100)])

    backup = cluster.make_backup("logical")
    cluster.enable_pitr(pitr_extra_args="--set pitr.oplogSpanMin=0.1")
    cluster.wait_pitr_chunk()
    start = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    client["test"]["test"].insert_many([{"phase": "post", "i": i} for i in range(100)])

    time.sleep(1)
    end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    cluster.disable_pitr(end)

    assert client["test"]["test"].count_documents({}) == 200

    cluster.make_restore(backup, check_pbm_status=True)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 100

    cluster.oplog_replay(start, end)
    client = pymongo.MongoClient(cluster.connection)
    assert client["test"]["test"].count_documents({}) == 200

    cluster.check_pbm_status()
    Cluster.log("Finished successfully")
