import os
from cluster import Cluster
from clustersync import Clustersync
from data_generator import generate_dummy_data

# To create setup:
# docker-compose run --env SETUP=true test python example_shard.py
# To destroy setup:
# docker-compose run --env CLEANUP=true test python example_shard.py

srcShard = Cluster({
    "mongos": "mongos1",
    "configserver": {"_id": "rscfg1", "members": [{"host": "rscfg101"}]},
    "shards": [
        {"_id": "rs1", "members": [{"host": "rs101"}]},
        {"_id": "rs2", "members": [{"host": "rs201"}]}
    ]
})
dstShard = Cluster({
    "mongos": "mongos2",
    "configserver": {"_id": "rscfg2", "members": [{"host": "rscfg201"}]},
    "shards": [
        {"_id": "rs3", "members": [{"host": "rs301"}]},
        {"_id": "rs4", "members": [{"host": "rs401"}]}
    ]
})
csync = Clustersync("csync", srcShard.csync_connection, dstShard.csync_connection)
setup = os.environ.get("SETUP", "").lower() == "true"
cleanup = os.environ.get("CLEANUP", "").lower() == "true"
if setup:
    srcShard.destroy()
    dstShard.destroy()
    csync.destroy()
    srcShard.create()
    dstShard.create()
    csync.create()
    generate_dummy_data(srcShard.connection, "test_db", 1, 20000, 10000)
    print("Setup complete")
elif cleanup:
    csync.destroy()
    srcShard.destroy()
    dstShard.destroy()
    print("Cleanup complete")

