import os
import re
import requests
import pytest
import json

PBM_VER = os.environ.get("PBM_VERSION")
SOFTWARE_FILES = ["bookworm", "bullseye", "binary", "redhat/9", "redhat/8", "source", "jammy", "noble", "redhat/2023"]


def get_package_tuples():
    list = []
    for software_files in SOFTWARE_FILES:
        data = (
            "version_files=percona-backup-mongodb-"
            + PBM_VER
            + "|percona-backup-mongodb&software_files="
            + software_files
        )
        req = requests.post(
            "https://www.percona.com/products-api.php",
            data=data,
            headers={"content-type": "application/x-www-form-urlencoded; charset=UTF-8"},
        )
        assert req.status_code == 200
        assert req.text != "[]", software_files
        if software_files == "binary":
            assert "percona-backup-mongodb-" + PBM_VER in req.text
        elif software_files == "source":
            assert "percona-backup-mongodb-" + PBM_VER + ".tar.gz" in req.text
        else:
            assert "percona-backup-mongodb-" + PBM_VER in req.text or "percona-backup-mongodb_" + PBM_VER in req.text
        files = json.loads(req.text)
        for file in files:
            list.append((software_files, file["filename"], file["link"]))
    return list


LIST_OF_PACKAGES = get_package_tuples()


@pytest.mark.parametrize(("software_files", "filename", "link"), LIST_OF_PACKAGES)
def test_packages_site(software_files, filename, link):
    print("\nTesting " + software_files + ", file: " + filename)
    print(link)
    req = requests.head(link)
    if not re.search(r"percona-backup-mongodb.*\.diff\.gz", link):
        assert req.status_code == 200 and int(req.headers["content-length"]) > 0, link
