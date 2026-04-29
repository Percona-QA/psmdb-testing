import os
import pytest
import re
import requests
from packaging import version
import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')


BINARIES = ['mongod', 'mongos', 'bsondump', 'mongoexport', 'mongobridge',
            'mongofiles', 'mongoimport', 'mongorestore', 'mongotop', 'mongostat']

PSMDB_VER = os.environ.get("PSMDB_VERSION")
TESTING_BRANCH = os.environ.get("TESTING_BRANCH")
MONGOSH_VER_RHEL7 = '2.1.5'

def get_mongosh_ver():
    url = "https://raw.githubusercontent.com/Percona-QA/psmdb-testing/" + TESTING_BRANCH + "/MONGOSH_VERSION"
    r = requests.get(url)
    return r.text.rstrip()

MONGOSH_VER = get_mongosh_ver()

def get_package_version(host):
    """Get full package version from system package manager"""
    package_names = ["percona-server-mongodb-server", "percona-server-mongodb-server-pro"]
    for pkg_name in package_names:
        if host.system_info.distribution.lower() in ["redhat", "centos", "rhel", "rocky", "almalinux", "ol", "amzn"]:
            pkg_result = host.run(f"rpm -q --queryformat '%{{VERSION}}-%{{RELEASE}}\\n' {pkg_name}")
            pattern = r'([\d\.]+-\d+)'
        else:
            pkg_result = host.run(f"dpkg -s {pkg_name} | grep '^Version:'")
            pattern = r'Version:\s*([\d\.]+-\d+)'
        if pkg_result.rc != 0:
            continue
        pkg_version = re.search(pattern, pkg_result.stdout)
        if pkg_version:
            return pkg_version.group(1)
    pytest.fail(f"Failed to determine installed package version from package manager. "
        f"Neither {package_names[0]} nor {package_names[1]} is installed.")

def test_mongod_service(host):
    mongod = host.service("mongod")
    assert mongod.is_running

def test_telemetry_service(host):
    parts = PSMDB_VER.split(".")
    major_version = ".".join(parts[:-1])
    match major_version:
        case "5.0":
         if version.parse(PSMDB_VER) <= version.parse("5.0.27"):
          pytest.skip("This version doesn't support telemetry")
        case "6.0":
         if version.parse(PSMDB_VER) <= version.parse("6.0.15"):
          pytest.skip("This version doesn't support telemetry")
        case "7.0":
         if version.parse(PSMDB_VER) <= version.parse("7.0.11"):
          pytest.skip("This version doesn't support telemetry")
    if not (host.system_info.distribution.lower() in ["redhat", "centos", 'rhel'] and host.system_info.release == '7'):
        telemetry = host.service("percona-telemetry-agent")
        assert telemetry.is_running

@pytest.mark.parametrize("binary", BINARIES)
def test_binary_version(host, binary):
    result = host.run(f"{binary} --version")
    output = result.stdout
    version_pattern = re.escape(PSMDB_VER) + r'-\d{1,2}'
    assert re.search(version_pattern, output), \
        f"Expected version pattern {PSMDB_VER}-XX not found in output: {output}"
    expected_version = get_package_version(host)
    # mongod, mongos, and mongobridge have extra version field outside build info
    for line in output.split('\n'):
        if 'version v' in line and ('mongos version' in line or 'db version' in line or 'mongobridge version' in line):
            match = re.search(r'(?:mongos|db|mongobridge) version v(\d+(?:\.\d+)*-\d{1,2})(?:\s|$)', line)
            if match:
                version_found = match.group(1)
                assert version_found == expected_version, \
                    f"Version {version_found} doesn't match {expected_version} (found in: {line})"
            else:
                assert False, f"Version line contains extra symbols or invalid version format in: {line}"
    if '"version":' in output:
        match = re.search(r'"version":\s*"(\d+(?:\.\d+)*-\d{1,2})"', output)
        if match:
            build_info_version = match.group(1)
            assert build_info_version == expected_version, \
                f"Build Info version {build_info_version} doesn't match {expected_version}"
        else:
            version_line = [line for line in output.split('\n') if '"version":' in line]
            assert False, f"Build Info version contains extra symbols or invalid version format in: {version_line[0] if version_line else 'not found'}"
    tool_version_match = re.search(r'^(?:.*?\s+)?version:\s+(\d+(?:\.\d+)*-\d{1,2})(?![.\w])', output, re.MULTILINE | re.IGNORECASE)
    if tool_version_match:
        tool_version = tool_version_match.group(1)
        assert tool_version == expected_version, \
            f"Tool version {tool_version} doesn't match {expected_version}"

def test_cli_version(host):
    result = host.check_output("mongo --version")
    if version.parse(PSMDB_VER) > version.parse("6.0.0"):
        if host.system_info.distribution.lower() in ["redhat", "centos", 'rhel'] and host.system_info.release == '7':
           assert MONGOSH_VER_RHEL7 in result
        else:
           assert MONGOSH_VER in result
    else:
        assert PSMDB_VER in result

