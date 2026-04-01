import json

import pytest
import pymongo
import time
import threading
from bson import ObjectId

from cluster import Cluster
from clustersync import Clustersync

@pytest.fixture(scope="module")
def dstRS():
    return Cluster({ "_id": "rs2", "members": [{"host":"rs201"}]},mongod_extra_args='--setParameter enableTestCommands=1')

@pytest.fixture(scope="module")
def srcRS():
    return Cluster({ "_id": "rs1", "members": [{"host":"rs101"}]},mongod_extra_args='--setParameter enableTestCommands=1 --oplogSize 20')

@pytest.fixture(scope="module")
def csync(srcRS,dstRS):
    return Clustersync('csync',srcRS.csync_connection + '&appName=pcsm', dstRS.csync_connection + '&appName=pcsm')

@pytest.fixture(scope="function")
def start_cluster(srcRS, dstRS, csync, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        csync.destroy()
        createSrc = threading.Thread(target=srcRS.create)
        createDst = threading.Thread(target=dstRS.create)
        createSrc.start()
        createDst.start()
        createSrc.join()
        createDst.join()
        csync.create()
        yield

    finally:
        srcRS.destroy()
        dstRS.destroy()
        csync.destroy()

def configure_failpoint_closeconn(connection,commands,mode):
    client = pymongo.MongoClient(connection)
    data = { 'closeConnection': True, 'failCommands': commands, 'appName': 'pcsm'}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data})
    Cluster.log(result)

def configure_failpoint_fulldisk(connection,commands,mode):
    client = pymongo.MongoClient(connection)
    data = { 'errorCode': 14031, 'failCommands': commands, 'appName': 'pcsm'}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data})
    Cluster.log(result)

def configure_failpoint_delay(connection,commands,mode,timeout):
    client = pymongo.MongoClient(connection)
    data = {'failCommands': commands, 'appName': 'pcsm', 'blockConnection': True, 'blockTimeMS': timeout}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data})
    Cluster.log(result)

def configure_failpoint_delay_all(connection,mode,timeout):
    client = pymongo.MongoClient(connection)
    data = {'failAllCommands': True, 'appName': 'pcsm', 'blockConnection': True, 'blockTimeMS': timeout}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data})
    Cluster.log(result)

def disable_failpoint(connection):
    client = pymongo.MongoClient(connection)
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': 'off'})
    Cluster.log(result)

@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T33(start_cluster, srcRS, dstRS, csync):
    """
    Test PCSM detection of ChangeStreamHistoryLost (insufficient oplog).
    Uses failCommand failpoint to inject error code 286 on the source
    getMore command, simulating an oplog that has been truncated past
    the change stream resume point.

    PCSM must detect the error and transition to the 'failed' state
    with error message 'change replication: oplog history is lost'.
    """
    csync.start()
    assert csync.wait_for_repl_stage(), "Failed to reach replication stage"
    # Inject ChangeStreamHistoryLost (error code 286) on the next getMore.
    # This simulates the oplog wrapping past the change stream cursor position.
    src = pymongo.MongoClient(srcRS.connection)
    result = src.admin.command({
        'configureFailPoint': 'failCommand',
        'mode': 'alwaysOn',
        'data': {
            'errorCode': 286,
            'failCommands': ['getMore'],
            'appName': 'pcsm'
        }
    })
    Cluster.log(result)
    status = None
    for _ in range(30):
        status = csync.status()
        Cluster.log(status)
        if not status['data']['ok'] and status['data']['state'] == 'failed':
            break
        time.sleep(1)
    disable_failpoint(srcRS.connection)
    assert not status['data']['ok']
    assert status['data']['state'] != 'running'
    assert status['data']['error'] == "change replication: oplog history is lost"
    # PCSM-300: verify CLI `pcsm status` returns full JSON in failed state
    exec_result = csync.container.exec_run("pcsm status", demux=True)
    stdout = exec_result.output[0].decode("utf-8", errors="replace") if exec_result.output[0] else ""
    stderr = exec_result.output[1].decode("utf-8", errors="replace") if exec_result.output[1] else ""
    try:
        cli_json = json.loads(stdout)
    except json.JSONDecodeError:
        pytest.fail(f"PCSM-300: 'pcsm status' did not return valid JSON in failed state.\n"
            f"STDOUT: {stdout}\nSTDERR: {stderr}")
    required_fields = ["state", "error", "eventsRead", "eventsApplied", "lagTimeSeconds", "initialSync"]
    missing = [f for f in required_fields if f not in cli_json]
    assert not missing, f"PCSM-300: CLI status missing fields: {missing}"
    assert cli_json["state"] == "failed"
    assert cli_json["error"] == status['data']['error']

@pytest.mark.timeout(300,func_only=True)
def test_csync_PML_T39(start_cluster, srcRS, dstRS, csync):
    """
    Test capped collection replication failure due to CappedPositionLost. Capped cursor returns
    CappedPositionLost if the document it was positioned on has been overwritten (deleted).
    Test adds data to small capped collection, while 25s delay is injected into all commands
    on the source cluster to simulate replication lag.
    """
    src = pymongo.MongoClient(srcRS.connection)
    stop_event = threading.Event()
    def capped_insert():
        src["dummy"].create_collection("capped_collection",capped=True,size=1024*1024*1024, max=2000)
        doc = {"x": 1, "y": "val", "z": [1, 2]}
        while True:
            try:
                if stop_event and stop_event.is_set():
                    break
                batch = [{**doc, "_id": ObjectId()} for _ in range(10000)]
                src["dummy"]["capped_collection"].insert_many(batch, ordered=False, bypass_document_validation=True)
            except Exception:
                break
    t1 = threading.Thread(target=capped_insert)
    assert csync.start(), "Failed to start csync service"
    assert csync.wait_for_repl_stage(), "Failed to start replication stage"
    t1.start()
    configure_failpoint_delay_all(srcRS.connection,'alwaysOn',30000)
    time.sleep(30)
    disable_failpoint(srcRS.connection)
    result = csync.wait_for_zero_lag(120)
    stop_event.set()
    t1.join()
    if not result:
        for _ in range(30):
            status = csync.status()
            Cluster.log(status)
            if not status['data']['ok'] and status['data']['state'] == 'failed':
                break
            time.sleep(1)
    assert not status['data']['ok']
    assert status['data']['state'] != 'running'
    assert status['data']['error'] == "change replication: oplog history is lost"