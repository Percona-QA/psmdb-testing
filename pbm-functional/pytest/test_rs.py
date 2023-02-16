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

def test_logical(start_cluster):
    nodes=["rs101","rs102","rs103"]
    backup=pbmhelper.make_backup(nodes[0],"logical")
    pbmhelper.make_restore(nodes[0],backup)

def test_physical(start_cluster):
    nodes=["rs101","rs102","rs103"]
    backup=pbmhelper.make_backup(nodes[0],"physical")
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
    
def test_incremental(start_cluster):
    nodes=["rs101","rs102","rs103"]
    pbmhelper.make_backup(nodes[0],"incremental --base")
    time.sleep(10)
    backup=pbmhelper.make_backup(nodes[0],"incremental")
    pbmhelper.make_restore(nodes[0],backup)
    mongohelper.restart_mongod(nodes)
    pbmhelper.restart_pbm_agents(nodes)
    pbmhelper.make_resync(nodes[0])
