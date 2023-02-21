import pytest
import subprocess
import testinfra
import time
import mongohelper
import pbmhelper

pytest_plugins = ["docker_compose"]

nodes=["rs101","rs102","rs103"]
newnodes=["newrs101","newrs102","newrs103"]

@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    time.sleep(5)
    rsname="rs"
    mongohelper.prepare_rs(rsname,nodes)
    mongohelper.setup_authorization(nodes[0])
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.setup_pbm(nodes[0])
    mongohelper.prepare_rs(rsname,newnodes)
    mongohelper.setup_authorization(newnodes[0])
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.setup_pbm(newnodes[0])

def test_logical(start_cluster):
    #perform backup on old cluster
    backup=pbmhelper.make_backup(nodes[0],"logical")
    #resync storage on new cluster
    pbmhelper.make_resync(newnodes[0])
    #perform restore on new cluster
    pbmhelper.make_restore(newnodes[0],backup)

def test_physical(start_cluster):
    backup=pbmhelper.make_backup(nodes[0],"physical")
    pbmhelper.make_resync(newnodes[0])
    pbmhelper.make_restore(newnodes[0],backup)
    mongohelper.restart_mongod(newnodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.make_resync(newnodes[0])

def test_incremental(start_cluster):
    pbmhelper.make_backup(nodes[0],"incremental --base")
    time.sleep(10)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    pbmhelper.make_resync(newnodes[0])
    pbmhelper.make_restore(newnodes[0],backup)
    mongohelper.restart_mongod(newnodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.make_resync(newnodes[0])
