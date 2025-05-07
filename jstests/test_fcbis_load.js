(function() {
    "use strict";

    load("jstests/replsets/rslib.js");
    load("jstests/libs/parallel_shell_helpers.js");

    const rst = new ReplSetTest({
        nodes: [{}, {rsConfig: {priority: 0}}],
    });

    rst.startSet();
    rst.initiate();

    const primary = rst.getPrimary();
    const secondary = rst.getSecondary();

    jsTestLog("Inserting initial data...");
    const dbName = "fcb_test_db";
    const collName = "coll";
    const testColl = primary.getDB(dbName)[collName];
    const initialDocs = [
        {_id: 1, a: "test"},
        {_id: 2, b: "more"},
        {_id: 3, c: [1, 2, 3]},
    ];
    assert.commandWorked(testColl.insertMany(initialDocs));

    jsTestLog("Enabling failCommand failpoint on $_backupFile on secondary...");

    const adminDB = secondary.getDB("admin");
    assert.commandWorked(adminDB.runCommand({
        configureFailPoint: "failCommand",
        mode: {skip: 0, times: 100},
        data: {
            failCommands: ["aggregate"],
            namespace: "admin.$cmd.aggregate",
//            closeConnection: true,
            blockConnection: true,
            blockTimeMS: 10,
            failInternalCommands: true,
            extraErrorInfo: {note: "Simulated connection delays on $_backupFile"}
        }
    }));

    jsTestLog("Starting background write load on primary...");
    var signal = primary.getDB('signal').signal;
    const updateWorkers = 10;
    var w = [];
    for (var i = 0; i < updateWorkers; i++) {
        w[i] = startParallelShell(function() {
            Random.setRandomSeed();
            db = db.getSiblingDB('db' + Random.randInt(10));
            var coll = db.getCollection('coll' + Random.randInt(10));
            var signal = db.getSiblingDB('signal').signal;
            while (signal.findOne() == null) {
                var key = 'key' + Random.randInt(1000);
                assert.writeOK(coll.insert({k: 1}));
            }
        }, primary.port );
    }

    jsTestLog("Adding new node to force initial sync from secondary...");
    const newNode = rst.add({
        rsConfig: {priority: 0, votes: 0},
        setParameter: {
            initialSyncMethod: "fileCopyBased"
        }
    });
    rst.reInitiate();

    jsTestLog("Waiting for new node to enter SECONDARY or fail...");
    waitForState(newNode, ReplSetTest.State.SECONDARY, 60 * 1000);  // 1 minute

    jsTestLog("Stopping load thread...");
    signal.insert({1: 1});

    for (var i = 0; i < updateWorkers; i++) {
        assert.eq(0, w[i](), 'wrong shell\'s exit code');
    }

    jsTestLog("Disabling failpoint...");
    adminDB.runCommand({configureFailPoint: "failCommand", mode: "off"});

/*
    jsTestLog("Checking data consistency...");
    const newColl = newNode.getDB(dbName)[collName];
    const primaryDocs = testColl.find().sort({_id: 1}).toArray();
    const newDocs = newColl.find().sort({_id: 1}).toArray();
    assert.eq(primaryDocs.length, newNodeDocs.length, "Document count mismatch after initial sync");
    assert.eq(primaryDocs, newNodeDocs, "Document content mismatch after initial sync");
*/
    jsTestLog("Test complete.");

    rst.stopSet();
})();
