import signal
from cluster import Cluster

#docker-compose run -ti --rm test python3 -i example_cluster.py

config = { "mongos": "mongos",
           "configserver":
                        {"_id": "rscfg", "members": [{"host": "rscfg01","tags": {"ce":"true"}}, {"host": "rscfg02"}, {"host": "rscfg03"}]},
           "shards":[
                        {"_id": "rs1", "members": [{"host": "rs101"}, {"host": "rs102"}, {"host": "rs103"}]},
                        {"_id": "rs2", "members": [{"host": "rs201"}, {"host": "rs202"}, {"host": "rs203"}]}
                    ]}

#mongod_extra_args = " --setParameter=perconaTelemetryGracePeriod=2 --setParameter=perconaTelemetryScrapeInterval=5"
mongod_extra_args = " "

cluster = Cluster(config,mongod_extra_args=mongod_extra_args)

def handler(signum,frame):
    cluster.destroy()
    exit(0)

cluster.destroy()
cluster.create()
#cluster.setup_pbm()

signal.signal(signal.SIGINT,handler)
print("\nCluster is prepared and ready to use")
print("\nPress CTRL-C to destroy and exit")

