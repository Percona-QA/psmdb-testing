import pytest
import pymongo
import time
import docker
import urllib.parse

from cluster import Cluster
from mongolink import Mongolink
from data_generator import create_all_types_db, stop_all_crud_operations
from data_integrity_check import compare_data_rs

@pytest.fixture(scope="package")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="package")
def mongod_extra_args():
    return '--tlsMode allowTLS --tlsCAFile=/etc/x509/ca.crt --tlsCertificateKeyFile=/etc/x509/psmdb.pem --setParameter=authenticationMechanisms=SCRAM-SHA-1,MONGODB-X509'

def _make_config(rs_name, host):
    return {"_id": rs_name, "members": [{"host": host}]}

@pytest.fixture(scope="package")
def srcRS(mongod_extra_args):
    config = _make_config("rs1", "rs101")
    return Cluster(config, mongod_extra_args=mongod_extra_args)

@pytest.fixture(scope="package")
def dstRS(mongod_extra_args):
    config = _make_config("rs2", "rs201")
    return Cluster(config, mongod_extra_args=mongod_extra_args)

@pytest.fixture(scope="function", params=[
    {
        "mode": "internal_auth",
        "options": {
            "appName": "pml",
            "tls": "true",
            "tlsCertificateKeyFile": "/etc/x509/pml.pem",
            "tlsInsecure": "true"
        }
    },
    {
        "mode": "internal_auth",
        "options": {
            "appName": "pml",
            "tls": "true",
            "tlsCertificateKeyFile": "/etc/x509/pml.pem",
            "tlsCAFile": "/etc/x509/ca.crt"
        }
    },
    {
        "mode": "external_auth",
        "options": {
            "appName": "pml",
            "authSource": "$external",
            "tls": "true",
            "tlsCertificateKeyFile": "/etc/x509/pml.pem",
            "tlsCAFile": "/etc/x509/ca.crt",
            "authMechanism": "MONGODB-X509"
        }
    }
])
def mlink_connection_options(request):
    return request.param

@pytest.fixture(scope="function")
def mlink(srcRS, dstRS, mlink_connection_options, request):
    options = urllib.parse.urlencode(mlink_connection_options["options"])
    if mlink_connection_options["mode"] == "internal_auth":
        src_uri = srcRS.mlink_connection + "&" + options
        dst_uri = dstRS.mlink_connection + "&" + options
    if mlink_connection_options["mode"] == "external_auth":
        src_uri = f"mongodb://rs101:27017/?{options}"
        dst_uri = f"mongodb://rs201:27017/?{options}"
    mlink_instance = Mongolink('mlink', src_uri, dst_uri, src_internal=srcRS.mlink_connection)
    def cleanup():
        mlink_instance.destroy()
    request.addfinalizer(cleanup)
    return mlink_instance

@pytest.fixture(scope="package")
def start_cluster(srcRS, dstRS):
    try:
        srcRS.destroy()
        dstRS.destroy()
        srcRS.create()
        dstRS.create()
        yield True
    finally:
        srcRS.destroy()
        dstRS.destroy()

@pytest.fixture(scope="function")
def reset_state(srcRS, dstRS, mlink, request):
    src_client = pymongo.MongoClient(srcRS.connection)
    dst_client = pymongo.MongoClient(dstRS.connection)
    def print_logs():
        if request.config.getoption("--verbose"):
            logs = mlink.logs()
            print(f"\n\nmlink Last 50 Logs for mlink:\n{logs}\n\n")
    request.addfinalizer(print_logs)
    mlink.destroy()
    for db_name in src_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            src_client.drop_database(db_name)
    for db_name in dst_client.list_database_names():
        if db_name not in {"admin", "local", "config"}:
            dst_client.drop_database(db_name)
    mlink.create()

@pytest.mark.timeout(300,func_only=True)
@pytest.mark.usefixtures("start_cluster")
def test_rs_mlink_PML_T45(reset_state, srcRS, dstRS, mlink, docker_client):
    """
    Test to check PML connection to DB with different URI options
    """
    try:
        _, operation_threads_1 = create_all_types_db(srcRS.connection, "init_test_db", start_crud=True)
        result = mlink.start()
        assert result is True, "Failed to start mlink service"
        # Check if all connections from PML are using correct appName
        mlink_container = docker_client.containers.get('mlink')
        mlink_network = list(mlink_container.attrs['NetworkSettings']['Networks'].values())[0]
        mlink_ip = mlink_network['IPAddress']
        for conn_str in [srcRS.connection, dstRS.connection]:
            client = pymongo.MongoClient(conn_str)
            active_ops = client.admin.command('currentOp', {"active": True})
            for op in active_ops.get('inprog', []):
                client_address = op.get('client', '')
                if not client_address:
                    continue
                client_ip = client_address.split(":")[0]
                app_name = op.get('clientMetadata', {}).get('application', {}).get('name', '')
                if client_ip == mlink_ip:
                    assert app_name == "pml", (f"Connection from {client_address} does not use appName=pml (found '{app_name}')")
            client.close()
        _, operation_threads_2 = create_all_types_db(srcRS.connection, "clone_test_db", start_crud=True)
        result = mlink.wait_for_repl_stage()
        assert result is True, "Failed to start replication stage"
        _, operation_threads_3 = create_all_types_db(srcRS.connection, "repl_test_db", start_crud=True)
        time.sleep(5)
    except Exception as e:
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
    result = mlink.wait_for_zero_lag()
    assert result is True, "Failed to catch up on replication"
    result = mlink.finalize()
    assert result is True, "Failed to finalize mlink service"
    result, _ = compare_data_rs(srcRS, dstRS)
    assert result is True, "Data mismatch after synchronization"
    mlink_error, error_logs = mlink.check_mlink_errors()
    assert mlink_error is True, f"Mlink reported errors in logs: {error_logs}"