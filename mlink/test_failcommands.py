import pytest
import pymongo
import time
import threading
import random

from cluster import Cluster
from mongolink import Mongolink
from metrics_collector import metrics_collector

@pytest.fixture(scope="package")
def dstRS():
    return Cluster({ "_id": "rs2", "members": [{"host":"rs201"}]},mongod_extra_args='--setParameter enableTestCommands=1')

@pytest.fixture(scope="package")
def srcRS():
    return Cluster({ "_id": "rs1", "members": [{"host":"rs101"}]},mongod_extra_args='--setParameter enableTestCommands=1 --oplogSize 20')

@pytest.fixture(scope="package")
def mlink(srcRS,dstRS):
    return Mongolink('mlink',srcRS.mlink_connection + '&appName=mongolink', dstRS.mlink_connection + '&appName=mongolink')

@pytest.fixture(scope="function")
def start_cluster(srcRS, dstRS, mlink, request):
    try:
        srcRS.destroy()
        dstRS.destroy()
        mlink.destroy()
        createSrc = threading.Thread(target=srcRS.create)
        createDst = threading.Thread(target=dstRS.create)
        createSrc.start()
        createDst.start()
        createSrc.join()
        createDst.join()
        mlink.create()
        yield

    finally:
        srcRS.destroy()
        dstRS.destroy()
        mlink.destroy()

def configure_failpoint_closeconn(connection,commands,mode):
    client = pymongo.MongoClient(connection)
    data = { 'closeConnection': True, 'failCommands': commands, 'appName': 'mongolink'}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data})
    Cluster.log(result)

def configure_failpoint_fulldisk(connection,commands,mode):
    client = pymongo.MongoClient(connection)
    data = { 'errorCode': 14031, 'failCommands': commands, 'appName': 'mongolink'}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data})
    Cluster.log(result)

def configure_failpoint_delay(connection,commands,mode,timeout):
    client = pymongo.MongoClient(connection)
    data = {'failCommands': commands, 'appName': 'mongolink', 'blockConnection': True, 'blockTimeMS': timeout}
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data})
    Cluster.log(result)

def disable_failpoint(connection):
    client = pymongo.MongoClient(connection)
    result = client.admin.command({'configureFailPoint': 'failCommand', 'mode': 'Off'})
    Cluster.log(result)

@pytest.mark.timeout(300,func_only=True)
def test_rs_mlink_PML_T33(start_cluster, srcRS, dstRS, mlink):
    """
    Test insufficient oplog for replication
    Configured delay on the destination cluster for bulkInsert command for 10 sec
    Configured oplogSize on the source 20 Mb
    Configured load with 2 threads each inserts 2Mb documents inside the loop
    """

    configure_failpoint_delay(dstRS.connection,['bulkWrite'],'alwaysOn',10000)

    def insert_docs(connection,db,coll,duration):
        client=pymongo.MongoClient(connection)
        timeout = time.time() + duration
        while insert:
            data = random.randbytes(2*1024*1024)
            client[db][coll].insert_one({ 'data': data})
            if time.time() > timeout:
                break

    insert = True
    background_insert1 = threading.Thread(target=insert_docs,args=(srcRS.connection,'test1','test',60,))
    background_insert2 = threading.Thread(target=insert_docs,args=(srcRS.connection,'test2','test',60,))
    background_insert1.start()
    background_insert2.start()
    mlink.start()
    background_insert1.join()
    background_insert2.join()
    mlink.wait_for_repl_stage(120)
    status = mlink.status()
    Cluster.log(status)
    assert status['data']['ok'] == False
    assert status['data']['state'] != 'running'

