import signal
import pymongo
from cluster import Cluster
from mongolink import Mongolink

"""
docker compose run -ti --rm test python3 -i example_cluster.py
The way to simulate network errors using failpoint 'failCommand'.
To enable failpoint based on appName use connection string with 'appName' parameter
Parameters passed to failpoint command:
'commands' - the array of commands to fail on like ['hello','find']
'mode' - can be either 'alwaysOn' or dictionary with 'skip' and 'times' like {skip: 1, times: 2}
There is the way not to close the connection but return known or unknown error to the client
See the examples in jstests/core/failcommand_failpoint.js
"""

dstRS = Cluster({ "_id": "rs2", "members": [{"host":"rs201"}]},mongod_extra_args='--setParameter enableTestCommands=1')
srcRS = Cluster({ "_id": "rs1", "members": [{"host":"rs101"}]},mongod_extra_args='--setParameter enableTestCommands=1')
mlink = Mongolink('mlink',srcRS.mlink_connection + '&appName=mongolink', dstRS.mlink_connection + '&appName=mongolink')

def configure_failpoint_failcommand(connection,commands,mode):
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

def handler(signum,frame):
    mlink.destroy()
    srcRS.destroy()
    dstRS.destroy()
    exit(0)

srcRS.destroy()
dstRS.destroy()
mlink.destroy()
srcRS.create()
dstRS.create()
mlink.create()
configure_failpoint_delay(srcRS.connection,['find','listIndexes','listDatabases','listCollections'],'alwaysOn',30000)
#configure_failpoint_fulldisk(dstRS.connection,['insert','update','bulkWrite'],'alwaysOn')

signal.signal(signal.SIGINT,handler)
print("\nCluster is prepared and ready to use")
print("\nPress CTRL-C to destroy and exit")

