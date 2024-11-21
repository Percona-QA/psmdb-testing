import os
import re
import requests
import pytest
import json
from packaging import version

PSMDB_VER = os.environ.get("PSMDB_VERSION")
MAJ_VER = PSMDB_VER.split("-")[0]
if version.parse(PSMDB_VER) > version.parse("7.0.0"):
    SOFTWARE_FILES = ['bookworm','bullseye','binary','redhat/9','redhat/8','source','jammy','focal']
elif version.parse(PSMDB_VER) > version.parse("6.0.0") and version.parse(PSMDB_VER) < version.parse("7.0.0"):
    SOFTWARE_FILES = ['bullseye','binary','redhat/9','redhat/8','source','jammy','focal']
    if (MAJ_VER.startswith("5") and version.parse(MAJ_VER) > version.parse("5.0.27")):
       SOFTWARE_FILES.append('noble')
    if (MAJ_VER.startswith("6") and version.parse(MAJ_VER) > version.parse("6.0.15")):
       SOFTWARE_FILES.append('noble')
    if (MAJ_VER.startswith("7") and version.parse(MAJ_VER) > version.parse("7.0.12")):
       SOFTWARE_FILES.append('noble')
else:
    SOFTWARE_FILES = ['bullseye','binary','redhat/8','source','jammy','focal']

def get_package_tuples():
    list = []
    for software_files in SOFTWARE_FILES:
        data = 'version_files=percona-server-mongodb-' + PSMDB_VER + '&software_files=' + software_files
        req = requests.post("https://www.percona.com/products-api.php",data=data,headers = {"content-type": "application/x-www-form-urlencoded; charset=UTF-8"})
        assert req.status_code == 200
        assert req.text != '[]', software_files
        if software_files == 'binary':
            if (MAJ_VER.startswith("5") and version.parse(MAJ_VER) > version.parse("5.0.27")) or \
               (MAJ_VER.startswith("6") and version.parse(MAJ_VER) > version.parse("6.0.15")) or \
               (MAJ_VER.startswith("7") and version.parse(MAJ_VER) > version.parse("7.0.12")):
                replacement_map = {'redhat/9': 'ol9','redhat/8': 'ol8','redhat/7': 'ol7'}
                tar_os = [replacement_map[os] if os in replacement_map else os for os in SOFTWARE_FILES if os not in ['source', 'binary']]
                for os in tar_os:
                  assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64." + os + "-minimal.tar.gz" in req.text
                  assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64." + os + "-minimal.tar.gz.sha256sum" in req.text
                  assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64." + os + ".tar.gz" in req.text
                  assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64." + os + ".tar.gz.sha256sum" in req.text
            else:
                assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64.glibc2.17-minimal.tar.gz" in req.text
                assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64.glibc2.17-minimal.tar.gz.sha256sum" in req.text
                assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64.glibc2.35-minimal.tar.gz" in req.text
                assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64.glibc2.35-minimal.tar.gz.sha256sum" in req.text
                assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64.glibc2.17.tar.gz" in req.text
                assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64.glibc2.17.tar.gz.sha256sum" in req.text
                assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64.glibc2.35.tar.gz" in req.text
                assert "percona-server-mongodb-" + PSMDB_VER + "-x86_64.glibc2.35.tar.gz.sha256sum" in req.text
        elif software_files == 'source':
            assert "percona-server-mongodb-" + PSMDB_VER + ".tar.gz" in req.text
        else:
            assert "percona-server-mongodb-" + PSMDB_VER + "." in req.text or "percona-server-mongodb_" + PSMDB_VER + "." in req.text
            assert "percona-server-mongodb-server-" + PSMDB_VER in req.text or "percona-server-mongodb-server_" + PSMDB_VER in req.text
            assert "percona-server-mongodb-mongos-" + PSMDB_VER in req.text or "percona-server-mongodb-mongos_" + PSMDB_VER in req.text
            assert "percona-server-mongodb-tools-" + PSMDB_VER in req.text or "percona-server-mongodb-tools_" + PSMDB_VER in req.text
            assert "percona-telemetry-agent" in req.text
            assert "dbg" in req.text or "debug" in req.text
            if version.parse(PSMDB_VER) > version.parse("6.0.0"):
                assert "mongosh" in req.text
            else:
                assert "percona-server-mongodb-shell-" + PSMDB_VER in req.text or "percona-server-mongodb-shell_" + PSMDB_VER in req.text
        files = json.loads(req.text)
        for file in files:
            list.append( (software_files,file['filename'],file['link']) )
    return list

LIST_OF_PACKAGES = get_package_tuples()

@pytest.mark.parametrize(('software_files','filename','link'),LIST_OF_PACKAGES)
def test_packages_site(software_files,filename,link):
    print('\nTesting ' + software_files + ', file: ' + filename)
    print(link)
    req = requests.head(link)
    if not re.search(r'percona-telemetry-agent.*\.diff\.gz', link):
        assert req.status_code == 200 and int(req.headers['content-length']) > 0, link
