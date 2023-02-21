import pytest
import testinfra
import time
import mongohelper
import pbmhelper
import docker

pytest_plugins = ["docker_compose"]

nodes = ["rscfg01", "rscfg02", "rscfg03", "rs101", "rs102", "rs103", "rs201", "rs202", "rs203"]
configsvr = { "rscfg": [ "rscfg01", "rscfg02", "rscfg03" ]}
sh01 = { "rs1": [ "rs101", "rs102", "rs103" ]}
sh02 = { "rs2": [ "rs201", "rs202", "rs203" ]}

@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    time.sleep(5)
    mongohelper.prepare_rs_parallel([configsvr, sh01, sh02])
    mongohelper.setup_authorization_parallel([sh01, sh02])
    time.sleep(5)
    mongos = testinfra.get_host("docker://mongos")
    result = mongos.check_output("mongo --quiet --eval 'sh.addShard( \"rs2/rs201:27017,rs202:27017,rs203:27017\" )'")
    print(result)
    result = mongos.check_output("mongo --quiet --eval 'sh.addShard( \"rs1/rs101:27017,rs102:27017,rs103:27017\" )'")
    print(result)
    mongohelper.setup_authorization("mongos")
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.setup_pbm(nodes[0])

def test_logical(start_cluster):
    mongos = testinfra.get_host("docker://mongos")
    result = mongos.check_output("mongo -u root -p root --eval 'sh.stopBalancer()' --quiet")
    print(result)
    backup=pbmhelper.make_backup(nodes[0],"logical")
    pbmhelper.make_restore(nodes[0],backup)
    result = mongos.check_output("mongo -u root -p root --eval 'sh.startBalancer()' --quiet")
    print(result)

def test_physical(start_cluster):
    backup=pbmhelper.make_backup(nodes[0],"physical")
    mongos = testinfra.get_host("docker://mongos")
    result = mongos.check_output("mongo -u root -p root --eval 'sh.stopBalancer()' --quiet")
    print(result)
#    docker.from_env().containers.get("mongos").stop()
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
#    docker.from_env().containers.get("mongos").start()
    result = mongos.check_output("mongo -u root -p root --eval 'sh.startBalancer()' --quiet")
    print(result)


def test_incremental(start_cluster):
    pbmhelper.make_backup(nodes[0],"incremental --base")
    time.sleep(10)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    mongos = testinfra.get_host("docker://mongos")
    result = mongos.check_output("mongo -u root -p root --eval 'sh.stopBalancer()' --quiet")
    print(result)
#    docker.from_env().containers.get("mongos").stop()
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
#    docker.from_env().containers.get("mongos").start()
    result = mongos.check_output("mongo -u root -p root --eval 'sh.startBalancer()' --quiet")
    print(result)

