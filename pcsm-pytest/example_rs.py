import os
from cluster import Cluster
from clustersync import Clustersync
from data_generator import generate_dummy_data

# To create setup:
# docker-compose run --env SETUP=true test python example_rs.py
# To destroy setup:
# docker-compose run --env CLEANUP=true test python example_rs.py

srcRS = Cluster({"_id": "rs1", "members": [{"host": "rs101"}]})
dstRS = Cluster({"_id": "rs2", "members": [{"host": "rs201"}]})
csync = Clustersync("csync", srcRS.csync_connection, dstRS.csync_connection)
setup = os.environ.get("SETUP", "").lower() == "true"
cleanup = os.environ.get("CLEANUP", "").lower() == "true"
if setup:
    srcRS.destroy()
    dstRS.destroy()
    csync.destroy()
    srcRS.create()
    dstRS.create()
    csync.create()
    generate_dummy_data(srcRS.connection, "test_db", 1, 20000, 10000)
    print("Setup complete")
elif cleanup:
    csync.destroy()
    srcRS.destroy()
    dstRS.destroy()
    print("Cleanup complete")
