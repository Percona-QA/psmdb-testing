import os
from cluster import Cluster
from perconalink import Perconalink
from data_generator import generate_dummy_data

# To create setup:
# docker-compose run --env SETUP=true test python example_rs.py
# To destroy setup:
# docker-compose run --env CLEANUP=true test python example_rs.py

srcRS = Cluster({"_id": "rs1", "members": [{"host": "rs101"}]})
dstRS = Cluster({"_id": "rs2", "members": [{"host": "rs201"}]})
plink = Perconalink("plink", srcRS.plink_connection, dstRS.plink_connection)
setup = os.environ.get("SETUP", "").lower() == "true"
cleanup = os.environ.get("CLEANUP", "").lower() == "true"
if setup:
    srcRS.destroy()
    dstRS.destroy()
    plink.destroy()
    srcRS.create()
    dstRS.create()
    plink.create()
    generate_dummy_data(srcRS.connection, "test_db", 1, 20000, 10000)
    print("Setup complete")
elif cleanup:
    plink.destroy()
    srcRS.destroy()
    dstRS.destroy()
    print("Cleanup complete")
