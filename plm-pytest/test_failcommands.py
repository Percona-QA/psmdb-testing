import pytest
import pymongo
import time
import threading
from bson import ObjectId

from cluster import Cluster
from perconalink import Perconalink
from data_generator import generate_dummy_data


@pytest.fixture(scope="module")
def dstRS():
    return Cluster(
        {"_id": "rs2", "members": [{"host": "rs201"}]}, mongod_extra_args="--setParameter enableTestCommands=1"
    )


@pytest.fixture(scope="module")
def srcRS():
    return Cluster(
        {"_id": "rs1", "members": [{"host": "rs101"}]},
        mongod_extra_args="--setParameter enableTestCommands=1 --oplogSize 20",
    )


@pytest.fixture(scope="module")
def plink(srcRS, dstRS):
    return Perconalink("plink", srcRS.plink_connection + "&appName=plm", dstRS.plink_connection + "&appName=plm")


@pytest.fixture(scope="function")
def start_cluster(srcRS, dstRS, plink, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        plink.destroy()
        createSrc = threading.Thread(target=srcRS.create)
        createDst = threading.Thread(target=dstRS.create)
        createSrc.start()
        createDst.start()
        createSrc.join()
        createDst.join()
        plink.create()
        yield

    finally:
        srcRS.destroy()
        dstRS.destroy()
        plink.destroy()


def configure_failpoint_closeconn(connection, commands, mode):
    client = pymongo.MongoClient(connection)
    data = {"closeConnection": True, "failCommands": commands, "appName": "plm"}
    result = client.admin.command({"configureFailPoint": "failCommand", "mode": mode, "data": data})
    Cluster.log(result)


def configure_failpoint_fulldisk(connection, commands, mode):
    client = pymongo.MongoClient(connection)
    data = {"errorCode": 14031, "failCommands": commands, "appName": "plm"}
    result = client.admin.command({"configureFailPoint": "failCommand", "mode": mode, "data": data})
    Cluster.log(result)


def configure_failpoint_delay(connection, commands, mode, timeout):
    client = pymongo.MongoClient(connection)
    data = {"failCommands": commands, "appName": "plm", "blockConnection": True, "blockTimeMS": timeout}
    result = client.admin.command({"configureFailPoint": "failCommand", "mode": mode, "data": data})
    Cluster.log(result)


def configure_failpoint_delay_all(connection, mode, timeout):
    client = pymongo.MongoClient(connection)
    data = {"failAllCommands": True, "appName": "plm", "blockConnection": True, "blockTimeMS": timeout}
    result = client.admin.command({"configureFailPoint": "failCommand", "mode": mode, "data": data})
    Cluster.log(result)


def disable_failpoint(connection):
    client = pymongo.MongoClient(connection)
    result = client.admin.command({"configureFailPoint": "failCommand", "mode": "off"})
    Cluster.log(result)


@pytest.mark.timeout(300, func_only=True)
def test_rs_plink_PML_T33(start_cluster, srcRS, dstRS, plink):
    """
    Test insufficient oplog for replication
    Configured delay on the destination cluster for bulkInsert command for 10 sec
    Configured oplogSize on the source 20 Mb
    Configured load - 5 million records, ~5.5 GB
    """
    configure_failpoint_delay(dstRS.connection, ["bulkWrite"], "alwaysOn", 10000)
    plink.start()
    generate_dummy_data(srcRS.connection, "dummy", 5, 1000000, 10000)
    plink.wait_for_zero_lag(120)
    for _ in range(30):
        status = plink.status()
        Cluster.log(status)
        if not status["data"]["ok"] and status["data"]["state"] == "failed":
            break
        time.sleep(1)
    assert status["data"]["ok"] == False
    assert status["data"]["state"] != "running"
    assert status["data"]["error"] == "change replication: oplog history is lost"


@pytest.mark.timeout(300, func_only=True)
def test_rs_plink_PML_T39(start_cluster, srcRS, dstRS, plink):
    """
    Test capped collection replication failure due to CappedPositionLost. Capped cursor returns
    CappedPositionLost if the document it was positioned on has been overwritten (deleted).
    Test adds data to small capped collection, while 25s delay is injected into all commands
    on the source cluster to simulate replication lag.
    """
    src = pymongo.MongoClient(srcRS.connection)
    stop_event = threading.Event()

    def capped_insert():
        src["dummy"].create_collection("capped_collection", capped=True, size=1024 * 1024 * 1024, max=2000)
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
    t1.start()
    result = plink.start()
    assert result is True, "Failed to start plink service"
    result = plink.wait_for_repl_stage()
    assert result is True, "Failed to start replication stage"
    configure_failpoint_delay_all(srcRS.connection, "alwaysOn", 30000)
    time.sleep(30)
    disable_failpoint(srcRS.connection)
    result = plink.wait_for_zero_lag(120)
    stop_event.set()
    t1.join()
    if not result:
        for _ in range(30):
            status = plink.status()
            Cluster.log(status)
            if not status["data"]["ok"] and status["data"]["state"] == "failed":
                break
            time.sleep(1)
    assert status["data"]["ok"] == False
    assert status["data"]["state"] != "running"
    assert status["data"]["error"] == "change replication: oplog history is lost"
