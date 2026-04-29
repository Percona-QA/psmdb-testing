import os
import re
import requests
import pytest

PCSM_VER = os.environ.get("PCSM_VERSION")
SOFTWARE_FILES = ['bookworm','bullseye','redhat/9','redhat/8','jammy','noble','redhat/2023', 'source']

PRODUCT_ID = 'percona-clustersync-mongodb'
DOWNLOADS_API_URL = "https://www.percona.com/wp-admin/admin-ajax.php"

def get_package_tuples():
    list = []
    version = 'percona-clustersync-mongodb-' + PCSM_VER
    for software_files in SOFTWARE_FILES:
        data = {
            'action': 'percona_downloads',
            'product_id': PRODUCT_ID,
            'version': version,
            'software': software_files,
        }
        req = requests.post(
            DOWNLOADS_API_URL,
            data=data,
            headers={"content-type": "application/x-www-form-urlencoded; charset=UTF-8"},
        )
        assert req.status_code == 200, software_files
        payload = req.json()
        assert payload.get('success') is True, software_files
        files = payload.get('data', {}).get('files', []) or []
        assert len(files) > 0, software_files
        body = req.text
        if software_files == 'binary':
            assert "percona-clustersync-mongodb-" + PCSM_VER in body
        else:
            assert "percona-clustersync-mongodb-" + PCSM_VER in body or "percona-clustersync-mongodb_" + PCSM_VER in body
        for file in files:
            list.append( (software_files,file['filename'],file['url']) )
    print(list)
    return list

LIST_OF_PACKAGES = get_package_tuples()

@pytest.mark.parametrize(('software_files','filename','link'),LIST_OF_PACKAGES)
def test_packages_site(software_files,filename,link):
    print('\nTesting ' + software_files + ', file: ' + filename)
    print(link)
    req = requests.head(link, allow_redirects=True)
    if not re.search(r'percona-clustersync-mongodb.*\.diff\.gz', link):
        assert req.status_code == 200 and int(req.headers['content-length']) > 0, link
