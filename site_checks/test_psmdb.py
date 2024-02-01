import os
import requests
import pytest
import json
from packaging import version

PSMDB_VER = os.environ.get("PSMDB_VERSION")
if version.parse(PSMDB_VER) > version.parse("7.0.0"):
    SOFTWARE_FILES = ['bookworm','bullseye','binary','redhat/9','redhat/8','redhat/7','source','jammy','focal']
elif version.parse(PSMDB_VER) > version.parse("6.0.0") and version.parse(PSMDB_VER) < version.parse("7.0.0"):
    SOFTWARE_FILES = ['bullseye','buster','binary','redhat/9','redhat/8','redhat/7','source','jammy','focal']
else:
    SOFTWARE_FILES = ['bullseye','buster','binary','redhat/8','redhat/7','source','jammy','focal']

def get_package_tuples():
    list = []
    for software_files in SOFTWARE_FILES:
        data = 'version_files=percona-server-mongodb-' + PSMDB_VER + '&software_files=' + software_files
        req = requests.post("https://www.percona.com/products-api.php",data=data,headers = {"content-type": "application/x-www-form-urlencoded; charset=UTF-8"})
        assert req.status_code == 200
        assert req.text != '[]', software_files
        if software_files == 'binary':
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
    assert req.status_code == 200 and int(req.headers['content-length']) > 0, link

