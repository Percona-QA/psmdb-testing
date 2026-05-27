import io
import json
import os
import subprocess
import tarfile
import tempfile

import requests

PCSM_VER = os.environ.get("PCSM_VERSION")

TARBALL_URL = (
    f"https://downloads.percona.com/downloads/percona-clustersync-mongodb/percona-clustersync-mongodb-{PCSM_VER}/binary/tarball/percona-clustersync-mongodb-{PCSM_VER}-x86_64.tar.gz"
)


def test_pcsm_tarball_contents():
    resp = requests.get(TARBALL_URL, stream=True)
    assert resp.status_code == 200, f"Failed to download tarball: {TARBALL_URL}"

    with tempfile.TemporaryDirectory() as tmpdir:
        with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz") as tar:
            tar.extractall(tmpdir)

        extracted = os.listdir(tmpdir)
        assert len(extracted) > 0, "Tarball extracted to an empty directory"

        root = os.path.join(tmpdir, extracted[0])
        contents = os.listdir(root)

        assert "pcsm" in contents, f"pcsm binary not found in tarball. Found: {contents}"
        assert "sbom.json" in contents, f"pcsm sbom not found in tarball. Found: {contents}"

        pcsm_binary = os.path.join(root, "pcsm")
        result = subprocess.run([pcsm_binary, "version"], capture_output=True, text=True)
        assert result.returncode == 0, f"pcsm version failed: {result.stderr}"

        with open(os.path.join(root, "sbom.json")) as f:
            sbom = json.load(f)
        assert sbom.get("bomFormat") == "CycloneDX", f"Unexpected bomFormat: {sbom.get('bomFormat')}"
        assert sbom.get("specVersion") == "1.6", f"Unexpected specVersion: {sbom.get('specVersion')}"
