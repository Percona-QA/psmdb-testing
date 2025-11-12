import pytest
import pymongo
import docker
import urllib.parse
import threading

from cluster import Cluster
from clustersync import Clustersync
from data_generator import create_all_types_db, stop_all_crud_operations
from data_integrity_check import compare_data

@pytest.fixture(scope="module")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="module")
def mongod_extra_args():
    return '--tlsMode allowTLS --tlsCAFile=/etc/x509/ca.crt --tlsCertificateKeyFile=/etc/x509/psmdb.pem --setParameter=authenticationMechanisms=SCRAM-SHA-1,MONGODB-X509'

@pytest.fixture(scope="module")
def mongos_extra_args():
    return '--tlsMode allowTLS --tlsCAFile=/etc/x509/ca.crt --tlsCertificateKeyFile=/etc/x509/psmdb.pem --setParameter=authenticationMechanisms=SCRAM-SHA-1,MONGODB-X509'

@pytest.fixture(scope="module")
def srcCluster(mongod_extra_args, mongos_extra_args):
    config = {
        "mongos": "mongos1",
        "configserver": {"_id": "rscfg1", "members": [{"host": "rscfg101"}]},
        "shards": [
            {"_id": "rs1", "members": [{"host": "rs101"}]},
            {"_id": "rs2", "members": [{"host": "rs201"}]}
        ]
    }
    return Cluster(config, mongod_extra_args=mongod_extra_args, mongos_extra_args=mongos_extra_args)

@pytest.fixture(scope="module")
def dstCluster(mongod_extra_args, mongos_extra_args):
    config = {
        "mongos": "mongos2",
        "configserver": {"_id": "rscfg2", "members": [{"host": "rscfg201"}]},
        "shards": [
            {"_id": "rs3", "members": [{"host": "rs301"}]},
            {"_id": "rs4", "members": [{"host": "rs401"}]}
        ]
    }
    return Cluster(config, mongod_extra_args=mongod_extra_args, mongos_extra_args=mongos_extra_args)

# Test case 1: check SCRAM authentication with TLS connection and tlsInsecure
# Test case 2: check SCRAM authentication with TLS connection and tlsCAFile
# Test case 3: check X509 authentication
@pytest.fixture(scope="function", params=[
    {
        "mode": "internal_auth",
        "options": {
            "appName": "pcsm",
            "tls": "true",
            "tlsCertificateKeyFile": "/etc/x509/pcsm.pem",
            "tlsInsecure": "true"
        }
    },
    {
        "mode": "internal_auth",
        "options": {
            "appName": "pcsm",
            "tls": "true",
            "tlsCertificateKeyFile": "/etc/x509/pcsm.pem",
            "tlsCAFile": "/etc/x509/ca.crt"
        }
    },
    {
        "mode": "external_auth",
        "options": {
            "appName": "pcsm",
            "authSource": "$external",
            "tls": "true",
            "tlsCertificateKeyFile": "/etc/x509/pcsm.pem",
            "tlsCAFile": "/etc/x509/ca.crt",
            "authMechanism": "MONGODB-X509"
        }
    }
])
def csync_connection_options(request):
    return request.param

@pytest.fixture(scope="function")
def csync(srcCluster, dstCluster, csync_connection_options, request):
    options = urllib.parse.urlencode(csync_connection_options["options"])
    if csync_connection_options["mode"] == "internal_auth":
        src_uri = srcCluster.csync_connection + "?" + options
        dst_uri = dstCluster.csync_connection + "?" + options
    if csync_connection_options["mode"] == "external_auth":
        src_uri = f"mongodb://mongos1:27017/?{options}"
        dst_uri = f"mongodb://mongos2:27017/?{options}"
    csync_instance = Clustersync('csync', src_uri, dst_uri, src_internal=srcCluster.csync_connection)
    def cleanup():
        csync_instance.destroy()
    request.addfinalizer(cleanup)
    return csync_instance

@pytest.fixture(scope="function")
def start_cluster(srcCluster, dstCluster, csync, request):
    try:
        srcCluster.destroy()
        dstCluster.destroy()
        csync.destroy()
        src_create_thread = threading.Thread(target=srcCluster.create)
        dst_create_thread = threading.Thread(target=dstCluster.create)
        src_create_thread.start()
        dst_create_thread.start()
        src_create_thread.join()
        dst_create_thread.join()
        csync.create()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            logs = csync.logs()
            print(f"\n\ncsync Last 50 Logs for csync:\n{logs}\n\n")
        srcCluster.destroy()
        dstCluster.destroy()
        csync.destroy()

@pytest.mark.timeout(300,func_only=True)
def test_shard_csync_PML_T45(start_cluster, srcCluster, dstCluster, csync, docker_client):
    """
    Test to check PCSM connection to DB with different URI options for sharded clusters
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcCluster.connection, "init_test_db", start_crud=True, is_sharded=True)
        assert csync.start(), "Failed to start csync service"
        _, operation_threads_2 = create_all_types_db(srcCluster.connection, "clone_test_db", start_crud=True, is_sharded=True)
        assert csync.wait_for_repl_stage(), "Failed to start replication stage"
        # Check if all connections from PCSM are using correct appName
        csync_container = docker_client.containers.get('csync')
        csync_network = list(csync_container.attrs['NetworkSettings']['Networks'].values())[0]
        csync_ip = csync_network['IPAddress']
        for conn_str in [srcCluster.connection, dstCluster.connection]:
            client = pymongo.MongoClient(conn_str)
            active_ops = client.admin.command('currentOp', {"active": True})
            for op in active_ops.get('inprog', []):
                client_address = op.get('client', '')
                if not client_address:
                    continue
                client_ip = client_address.split(":")[0]
                app_name = op.get('clientMetadata', {}).get('application', {}).get('name', '')
                if client_ip == csync_ip:
                    assert app_name == "pcsm", (f"Connection from {client_address} does not use appName=pcsm (found '{app_name}')")
            client.close()
        _, operation_threads_3 = create_all_types_db(srcCluster.connection, "repl_test_db", start_crud=True, is_sharded=True)
    except Exception:
        raise
    finally:
        stop_all_crud_operations()
        all_threads = []
        if "operation_threads_1" in locals():
            all_threads += operation_threads_1
        if "operation_threads_2" in locals():
            all_threads += operation_threads_2
        if "operation_threads_3" in locals():
            all_threads += operation_threads_3
        for thread in all_threads:
            thread.join()
    assert csync.wait_for_zero_lag(), "Failed to catch up on replication"
    assert csync.finalize(), "Failed to finalize csync service"
    result, _ = compare_data(srcCluster, dstCluster)
    assert result is True, "Data mismatch after synchronization"
    csync_error, error_logs = csync.check_csync_errors()
    assert csync_error is True, f"Csync reported errors in logs: {error_logs}"

