import os

import pytest
import testinfra.utils.ansible_runner
from packaging import version

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')


BINARIES = ['mongod', 'mongos', 'bsondump', 'mongoexport', 'mongobridge',
            'mongofiles', 'mongoimport', 'mongorestore', 'mongotop', 'mongostat']

psmdb_version = os.environ["PSMDB_VERSION"]

JSTESTS = ['test_kerberos_simple.js', 'test_ldap_simple.js']
if version.parse(psmdb_version) >= version.parse("7.0.0"):
    JSTESTS.append('test_oidc_simple.js')
SUITES = ['multiversion_kmip', 'multiversion_vault']
FIPS = ['ssl jstests/ssl/ssl_fips.js']

def is_ubuntu_pro(host):
    proStatus = host.run("sudo pro status")
    return "This machine is not attached to an Ubuntu Pro subscription." not in proStatus.stdout

@pytest.mark.parametrize("binary", BINARIES)
def test_binary_version(host, binary):
    result = host.check_output(f"/usr/bin/{binary} --version")
    assert psmdb_version in result, f"{result}"

@pytest.mark.parametrize("jstest", JSTESTS)
def test_jstests(host, jstest):
    cmd = "cd /percona-server-mongodb && /opt/venv/bin/python buildscripts/resmoke.py run --suite no_server /package-testing/jstests/"  + jstest
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
    assert result.rc == 0, result.stdout

@pytest.mark.parametrize("suites", SUITES)
def test_suites(host, suites):
    cmd = "cd /percona-server-mongodb && /opt/venv/bin/python buildscripts/resmoke.py run --suite "  + suites
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
    assert result.rc == 0, result.stdout

@pytest.mark.parametrize("fips", FIPS)
def test_fips(host, fips):
    if host.system_info.distribution == "debian" or (host.system_info.distribution == "ubuntu" and not (is_ubuntu_pro(host) and "22.04" in host.system_info.release)):
        pytest.skip("Skip debian12 as no openssl with FIPS available")
    if version.parse(psmdb_version) >= version.parse("8.3.0"):
        pytest.skip("PSMDB 8.3+ requires additional ssl certs setup")
    cmd = f"cd /percona-server-mongodb && /opt/venv/bin/python buildscripts/resmoke.py run --suite {fips}"
    with host.sudo():
        result = host.run(cmd)
        print(result.stderr)
    assert result.rc == 0, result.stdout

def test_psmdb_sbom(host):
    """Verify SBOM exists in the tarball and is valid CycloneDX 1.6; report vulnerability scan (non-fatal)"""
    sbom_path = "/usr/doc/sbom.cdx.json"
    assert host.file(sbom_path).exists, f"SBOM not found in tarball at {sbom_path}"

    # grype (not trivy) recognises the `github` type components in PSMDB's SBOM.
    # Report vulnerabilities only; do not fail the test if any are found.
    grype_result = host.run(f"grype sbom:{sbom_path} --only-fixed")
    print(f"grype scan result:\n{grype_result.stdout}\n{grype_result.stderr}")

    cdx_cmd = "DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 /usr/local/bin/cyclonedx"
    cdx_result = host.run(f"{cdx_cmd} validate --input-file {sbom_path} --input-format json --input-version v1_6")
    assert cdx_result.rc == 0, f"CycloneDX 1.6 schema validation failed: {cdx_result.stdout}\n{cdx_result.stderr}"