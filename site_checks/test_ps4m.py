import os
import re

import pytest
import requests

PS4M_VER = os.environ.get("PS4M_VERSION")
# The download-site release directory is named after the upstream version only
# (e.g. percona-search-mongodb-1.70.3), while the package files carry the full
# version-revision (e.g. percona-search-mongodb_1.70.3-1.bookworm_amd64.deb).
MONGOT_VER = PS4M_VER.split("-")[0]
SOFTWARE_FILES = ['bookworm','jammy','noble','binary','redhat/9','redhat/8','redhat/2023','source']

PRODUCT_ID = 'percona-search-mongodb'
DOWNLOADS_API_URL = "https://www.percona.com/wp-admin/admin-ajax.php"

MISSING_SOFTWARE = []

def get_package_tuples():
    list = []
    version = 'percona-search-mongodb-' + MONGOT_VER
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
        if len(files) == 0:
            MISSING_SOFTWARE.append(software_files)
            continue
        body = req.text
        assert "percona-search-mongodb-" + MONGOT_VER in body or "percona-search-mongodb_" + MONGOT_VER in body, software_files
        for file in files:
            list.append( (software_files,file['filename'],file['url']) )
    print(list)
    return list

LIST_OF_PACKAGES = get_package_tuples()

def test_all_software_available():
    assert not MISSING_SOFTWARE, "No packages published for: " + ", ".join(MISSING_SOFTWARE)

@pytest.mark.parametrize(('software_files','filename','link'),LIST_OF_PACKAGES)
def test_packages_site(software_files,filename,link):
    print('\nTesting ' + software_files + ', file: ' + filename)
    print(link)
    req = requests.head(link, allow_redirects=True)
    if not re.search(r'percona-search-mongodb.*\.diff\.gz', link):
        assert req.status_code == 200 and int(req.headers['content-length']) > 0, link
