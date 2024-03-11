(function() {
"use strict";

const telmPath = "/usr/local/percona/telemetry/psmdb";
const setParameterOpts = {
    perconaTelemetryGracePeriod: 2,
    perconaTelemetryScrapeInterval: 5,
    perconaTelemetryHistoryKeepInterval: 9
};

var cleanupTelmDir = function() {
    var files = listFiles(telmPath);
    files.forEach((file) => {
        removeFile(file.name)
    });
};

var getTelmRawData = function() {
    var files = listFiles(telmPath);
    var data = '';
    files.forEach((file) => {
        data = data + cat(file.name)
    });
    return data;
};

var getTelmInstanceId = function(conn) {
    var cmdLineOpts = conn.getDB("admin").runCommand({getCmdLineOpts: 1});
    var dbPath = cmdLineOpts['parsed']['storage']['dbPath'];
    var telmId = _readDumpFile(dbPath + "/psmdb_telemetry.data");
    return telmId[0]['db_instance_id'].str;
};

var getTelmDataByConn = function(conn) {
    var id = getTelmInstanceId(conn);
    var files = listFiles(telmPath);
    var data = [] ;
    files.forEach((file) => {
        if (file.name.includes(id)) {
            data.push(JSON.parse(cat(file.name)))
        }
    });
    return data;
};

var telmTestSingle = function() {
    mkdir(telmPath);
    cleanupTelmDir();

    var singleTest = MongoRunner.runMongod({
        setParameter: setParameterOpts
    });

    //test perconaTelemetryGracePeriod
    sleep(3000);
    var telmFileList = listFiles(telmPath);
    assert.eq(1,telmFileList.length,telmFileList);

    //test telemetry data
    var jsonTelmData = getTelmDataByConn(singleTest);
    jsTest.log("Get single-node telemetry");
    jsTest.log(jsonTelmData);

    assert(jsonTelmData['pro_features'],"pro_features doesn't exist");
    if ( jsonTelmData['pro_features'].length > 0 ) {
        assert.eq('mongod-pro',jsonTelmData['source'],jsonTelmData['source']);
    } else {
        assert.eq('mongod',jsonTelmData['source'],jsonTelmData['source']);
    }
    assert.eq('wiredTiger',jsonTelmData['storage_engine'],jsonTelmData['storage_engine']);
    assert(jsonTelmData['db_instance_id'],"db_instance_id doesn't exist");
    assert(jsonTelmData['db_internal_id'],"db_internal_id doesn't exist");
    assert(jsonTelmData['pillar_version'],"pillar_version doesn't exist");
    assert(jsonTelmData['uptime'],"uptime doesn't exist");

    //test perconaTelemetryScrapeInterval
    sleep(5000);
    telmFileList = listFiles(telmPath);
    assert.eq(2,telmFileList.length,telmFileList);

    //test perconaTelemetryHistoryKeepInterval
    sleep(5000);
    telmFileList = listFiles(telmPath);
    assert.eq(2,telmFileList.length,telmFileList);

    //test disable perconaTelemetry
    assert.commandWorked(singleTest.getDB("admin").runCommand({setParameter: 1, "perconaTelemetry": false}));
    cleanupTelmDir();
    sleep(6000);
    telmFileList = listFiles(telmPath);
    assert.eq(0,telmFileList.length,telmFileList);

    //test enable perconaTelemetry
    assert.commandWorked(singleTest.getDB("admin").runCommand({setParameter: 1, "perconaTelemetry": true}));
    sleep(6000);
    telmFileList = listFiles(telmPath);
    assert.eq(1,telmFileList.length,telmFileList);

    MongoRunner.stopMongod(singleTest);
};

var telmTestRepl = function() {
    mkdir(telmPath);
    cleanupTelmDir();

    var replTest = new ReplSetTest({
        nodeOptions: { setParameter: setParameterOpts },
        nodes: [
            {/* primary */},
            {/* secondary */ rsConfig: {priority: 0}},
            {/* arbiter */ rsConfig: {arbiterOnly: true}}
        ]
    });
    replTest.startSet();
    replTest.initiate();

    sleep(3000);

    var telmFileList = listFiles(telmPath);
    assert.eq(3,telmFileList.length,telmFileList);

    //test replication_state
    var telmData = getTelmData();
    jsTest.log("Get RS tetemetry");
    jsTest.log(telmData);
    var primaryTelmData = getTelmDataByConn(replTest.nodes[0])[0];
    var secondaryTelmData = getTelmDataByConn(replTest.nodes[1])[0];
    var arbiterTelmData = getTelmDataByConn(replTest.nodes[2])[0];
    var dbReplicationId = primaryTelmData['db_replication_id'];
    assert.eq(primaryTelmData['replication_state'],'PRIMARY');
    assert.eq(secondaryTelmData['replication_state'],'SECONDARY');
    assert.eq(arbiterTelmData['replication_state'],'ARBITER');
    assert.eq(secondaryTelmData['db_replication_id'],dbReplicationId);
    assert.eq(arbiterTelmData['db_replication_id'],dbReplicationId);

    replTest.stopSet();
};

var telmTestSharding = function() {
    mkdir(telmPath);
    cleanupTelmDir();

    var st = new ShardingTest({
        shards: 1,
        config: 1,
        mongos: 1,
        rs: { nodes: 1, setParameter: setParameterOpts },
        mongosOptions: { setParameter: setParameterOpts },
        configOptions: { setParameter: setParameterOpts }
    });

    sleep(3000);

    //test mongos + config_svr + shard_svr
    var telmFileList = listFiles(telmPath);
    assert.eq(3,telmFileList.length,telmFileList)
    var telmData = getTelmRawData();
    jsTest.log("Get sharded cluster telemetry");
    jsTest.log(telmData)
    assert.includes(telmData,'mongos');
    assert.includes(telmData,'"shard_svr": "true"');
    assert.includes(telmData,'"config_svr": "true"');

    st.stop();
};

telmTestSingle();
telmTestRepl();
telmTestSharding();
}());
