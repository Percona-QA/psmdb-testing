import pytest
import subprocess
import testinfra
import time
import mongohelper
import pbmhelper

pytest_plugins = ["docker_compose"]

@pytest.fixture(scope="function")
def start_cluster(function_scoped_container_getter):
    rsname="rs"
    nodes=["rs101","rs102","rs103"]
    mongohelper.prepare_rs(rsname,nodes,'none')
    pbmhelper.configure_pbm_agents(rsname,nodes)
    pbmhelper.setup_pbm(nodes[0])
    newnodes=["newrs101","newrs102","newrs103"]
    mongohelper.prepare_rs(rsname,newnodes,'none')
    pbmhelper.configure_pbm_agents(rsname,newnodes)
    pbmhelper.setup_pbm(newnodes[0])

def test_logical(start_cluster):
    nodes=["rs101","rs102","rs103"]
    #perform backup on old cluster
    backup=pbmhelper.make_backup(nodes[0],"logical")
    newnodes=["newrs101","newrs102","newrs103"]
    #resync storage on new cluster
    pbmhelper.make_resync(newnodes[0])
    #perform restore on new cluster
    pbmhelper.make_restore(newnodes[0],backup)

def test_physical(start_cluster):
    nodes=["rs101","rs102","rs103"]
    backup=pbmhelper.make_backup(nodes[0],"physical")
    newnodes=["newrs101","newrs102","newrs103"]
    pbmhelper.make_resync(newnodes[0])
    pbmhelper.make_restore(newnodes[0],backup)
    mongohelper.restart_mongod(newnodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.make_resync(newnodes[0])

def test_incremental(start_cluster):
    nodes=["rs101","rs102","rs103"]
    pbmhelper.make_backup(nodes[0],"incremental --base")
    time.sleep(10)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    newnodes=["newrs101","newrs102","newrs103"]
    pbmhelper.make_resync(newnodes[0])
    pbmhelper.make_restore(newnodes[0],backup)
    mongohelper.restart_mongod(newnodes)
    pbmhelper.restart_pbm_agents(newnodes)
    pbmhelper.make_resync(newnodes[0])
