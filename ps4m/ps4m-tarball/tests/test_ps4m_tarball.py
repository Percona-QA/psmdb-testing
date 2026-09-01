import os

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

RESMOKE_SUITE = 'mongot_community_fixture_e2e_single_node'
INSTALL_DIR = '/usr/bin'
BUNDLE_DIR = f'{INSTALL_DIR}/mongot-community'
MONGOT_BIN = f'{BUNDLE_DIR}/mongot'
BUNDLE_FILES = (
    'VERSION.txt',
    'config.default.yml',
    'bin/jdk/bin/java',
    'bin/mongot_community_deploy.jar',
)
EMBEDDING_CATALOG = 'embedding-service-configs.yml'
# The launcher runs the JVM with -Djava.io.tmpdir=${TMPDIR:-/var/tmp}, because bc-fips
# extracts its JNI probe there and dlopens it, which fails on a noexec /tmp (RHEL 8+).
# The rpm/deb set TMPDIR=/var/lib/mongot/tmp for the service; a tarball install has no
# unit file, so the tests export it themselves. Kept in sync with the role.
MONGOT_TMPDIR = '/var/lib/mongot/tmp'
MONGOT_ENV = f'TMPDIR={MONGOT_TMPDIR}'


def test_mongot_bundle_layout(host):
    with host.sudo():
        binary = host.file(MONGOT_BIN)
        assert binary.exists, f'{MONGOT_BIN} is missing'
        assert binary.mode & 0o111, oct(binary.mode)
        for name in BUNDLE_FILES:
            assert host.file(f'{BUNDLE_DIR}/{name}').exists, name
        if EMBEDDING_CATALOG in binary.content_string:
            assert host.file(f'{BUNDLE_DIR}/{EMBEDDING_CATALOG}').exists

def test_mongot_version(host):
    with host.sudo():
        version_file = host.file(f'{BUNDLE_DIR}/VERSION.txt')
        expected = version_file.content_string.split()[-1]
        result = host.run(f'{MONGOT_ENV} {MONGOT_BIN} --version')
        assert result.rc == 0, result.stderr
        assert expected in result.stdout, result.stdout


def test_mongot_community_fixture_e2e_single_node(host):
    cmd = (
        "cd /percona-server-mongodb && "
        f"{MONGOT_ENV} /opt/venv/bin/python buildscripts/resmoke.py run "
        f"--suite {RESMOKE_SUITE} --installDir {INSTALL_DIR} --continueOnFailure"
    )
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
    assert result.rc == 0, result.stdout
