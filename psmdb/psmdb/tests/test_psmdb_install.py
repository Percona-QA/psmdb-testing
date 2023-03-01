import os
import pytest
import json
import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

DEB_PACKAGES = ['percona-server-mongodb', 'percona-server-mongodb-server', 'percona-server-mongodb-mongos',
                'percona-server-mongodb-shell', 'percona-server-mongodb-tools', 'percona-server-mongodb-dbg']
RPM_PACKAGES = ['percona-server-mongodb', 'percona-server-mongodb-server', 'percona-server-mongodb-mongos',
                'percona-server-mongodb-shell', 'percona-server-mongodb-tools', 'percona-server-mongodb-debuginfo']
RPM_NEW_CENTOS_PACKAGES = ['percona-server-mongodb', 'percona-server-mongodb-mongos-debuginfo',
                           'percona-server-mongodb-server-debuginfo', 'percona-server-mongodb-shell-debuginfo',
                           'percona-server-mongodb-tools-debuginfo', 'percona-server-mongodb-debugsource']

BINARIES = ['mongo', 'mongod', 'mongos', 'bsondump', 'mongoexport', 'mongobridge',
            'mongofiles', 'mongoimport', 'mongorestore', 'mongotop', 'mongostat']

PSMDB_VER = os.environ.get("PSMDB_VERSION")
toolkit = os.environ.get("ENABLE_TOOLKIT")

def test_version(host):
    if toolkit != "true" :
        pytest.skip("skipping pt tests")
    cmd = "pt-mongodb-summary -f json 2>/dev/null"
    result = host.run(cmd)
    version = json.loads(result.stdout)['HostInfo']['Version']
    print("mongod version is: " + version)
    assert result.rc == 0
    assert PSMDB_VER in version

def test_functional(host):
    cmd = "/package-testing/scripts/psmdb_test.sh" + ' ' + PSMDB_VER.split('.')[0] + '.' + PSMDB_VER.split('.')[1]
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_enable_auth(host):
    cmd = "/package-testing/scripts/psmdb_set_auth.sh"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_bats(host):
    cmd = "/usr/local/bin/bats /package-testing/bats/mongo-init-scripts.bats"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout


def test_bats_with_numactl(host):
    with host.sudo():
        os = host.system_info.distribution
        cmd = 'apt-get install numactl -y'
        if os.lower() in ["redhat", "centos", 'rhel']:
            cmd = 'yum install numactl -y'
        result = host.run(cmd)
        assert result.rc == 0, result.stdout
        cmd = "/usr/local/bin/bats /package-testing/bats/mongo-init-scripts.bats"
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_keyfile_encryption(host):
    cmd = "/package-testing/scripts/psmdb_encryption/psmdb-encryption-test.sh keyfile"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_vault_encryption(host):
    cmd = "/package-testing/scripts/psmdb_encryption/psmdb-encryption-test.sh vault"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_kmip_encryption(host):
    cmd = "/package-testing/scripts/psmdb_encryption/psmdb-encryption-test.sh kmip"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_ldap_native(host):
#    print(host.system_info.arch)
#    if host.system_info.arch in ["arm64","aarch64"]:
#        pytest.skip("skipping test on ARM arch")
    cmd = "/package-testing/scripts/psmdb_ldap/psmdb_test_ldap.sh"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_kerberos(host):
    cmd = "/package-testing/scripts/psmdb_kerberos/psmdb_test_kerberos.sh"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_load_data(host):
    cmd = "/package-testing/scripts/mongo_insert.sh"
    with host.sudo():
        result = host.run(cmd)
        print(result.stdout)
        print(result.stderr)
    assert result.rc == 0, result.stdout
