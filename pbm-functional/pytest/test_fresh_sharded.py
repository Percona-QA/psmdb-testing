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
newnodes = ["newrscfg01", "newrscfg02", "newrscfg03", "newrs101", "newrs102", "newrs103", "newrs201", "newrs202", "newrs203"]
newconfigsvr = { "rscfg": [ "newrscfg01", "newrscfg02", "newrscfg03" ]}
newsh01 = { "rs1": [ "newrs101", "newrs102", "newrs103" ]}
newsh02 = { "rs2": [ "newrs201", "newrs202", "newrs203" ]}

@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    time.sleep(5)
    mongohelper.prepare_rs_parallel([configsvr, sh01, sh02, newconfigsvr, newsh01, newsh02])
    mongohelper.setup_authorization_parallel([sh01, sh02, newsh01, newsh02])
    time.sleep(5)
    mongos = testinfra.get_host("docker://mongos")
    result = mongos.check_output("mongo --quiet --eval 'sh.addShard( \"rs2/rs201:27017,rs202:27017,rs203:27017\" )'")
    print(result)
    result = mongos.check_output("mongo --quiet --eval 'sh.addShard( \"rs1/rs101:27017,rs102:27017,rs103:27017\" )'")
    print(result)
    newmongos = testinfra.get_host("docker://newmongos")
    result = newmongos.check_output("mongo --quiet --eval 'sh.addShard( \"rs2/newrs201:27017,newrs202:27017,newrs203:27017\" )'")
    print(result)
    result = newmongos.check_output("mongo --quiet --eval 'sh.addShard( \"rs1/newrs101:27017,newrs102:27017,newrs103:27017\" )'")
    print(result)
    mongohelper.setup_authorization("mongos")
    mongohelper.setup_authorization("newmongos")
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.setup_pbm(nodes[0])
    pbmhelper.setup_pbm(newnodes[0])

def test_logical(start_cluster):
    backup=pbmhelper.make_backup(nodes[0],"logical")
    pbmhelper.make_resync(newnodes[0])
    newmongos = testinfra.get_host("docker://newmongos")
    result = newmongos.check_output("mongo -u root -p root --eval 'sh.stopBalancer()' --quiet")
    print(result)
    pbmhelper.make_restore(newnodes[0],backup)
    result = newmongos.check_output("mongo -u root -p root --eval 'sh.startBalancer()' --quiet")
    print(result)

def test_physical(start_cluster):
    backup=pbmhelper.make_backup(nodes[0],"physical")
    pbmhelper.make_resync(newnodes[0])
    newmongos = testinfra.get_host("docker://newmongos")
    result = newmongos.check_output("mongo -u root -p root --eval 'sh.stopBalancer()' --quiet")
    print(result)
    pbmhelper.make_restore(newnodes[0],backup)
    mongohelper.restart_mongod(newnodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.make_resync(newnodes[0])
    result = newmongos.check_output("mongo -u root -p root --eval 'sh.startBalancer()' --quiet")
    print(result)

def test_incremental(start_cluster):
    pbmhelper.make_backup(nodes[0],"incremental --base")
    time.sleep(10)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    pbmhelper.make_resync(newnodes[0])
    newmongos = testinfra.get_host("docker://newmongos")
    result = newmongos.check_output("mongo -u root -p root --eval 'sh.stopBalancer()' --quiet")
    print(result)
    pbmhelper.make_restore(newnodes[0],backup)
    mongohelper.restart_mongod(newnodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.make_resync(newnodes[0])
    result = newmongos.check_output("mongo -u root -p root --eval 'sh.startBalancer()' --quiet")
    print(result)

