import io
import os
import subprocess
import tarfile
import tempfile

import requests

PCSM_VER = os.environ.get("PCSM_VERSION")
assert PCSM_VER, "PCSM_VERSION environment variable must be set"

INSTALL_REPO = os.environ.get("install_repo", "testing")

TARBALL_FILENAME = f"percona-clustersync-mongodb-{PCSM_VER}-x86_64.tar.gz"
TARBALL_URLS = {
    "release":      f"https://downloads.percona.com/downloads/percona-clustersync-mongodb/percona-clustersync-mongodb-{PCSM_VER}/binary/tarball/{TARBALL_FILENAME}",
    "testing":      f"https://downloads.percona.com/downloads/TESTING/pcsm-{PCSM_VER}/{TARBALL_FILENAME}",
    "experimental": f"https://downloads.percona.com/downloads/EXPERIMENTAL/pcsm-{PCSM_VER}/{TARBALL_FILENAME}",
}
assert INSTALL_REPO in TARBALL_URLS, "install_repo must be release, testing, or experimental"
TARBALL_URL = TARBALL_URLS[INSTALL_REPO]

def test_pcsm_tarball_contents():
    resp = requests.get(TARBALL_URL, stream=True, timeout=60)
    assert resp.status_code == 200, f"Failed to download tarball: {TARBALL_URL}"

    with tempfile.TemporaryDirectory() as tmpdir:
        with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz") as tar:
            tar.extractall(tmpdir, filter='data')

        extracted = os.listdir(tmpdir)
        assert len(extracted) > 0, "Tarball extracted to an empty directory"

        root = os.path.join(tmpdir, extracted[0])
        contents = os.listdir(root)

        assert "pcsm" in contents, f"pcsm binary not found in tarball. Found: {contents}"

        pcsm_binary = os.path.join(root, "pcsm")
        result = subprocess.run([pcsm_binary, "version"], capture_output=True, text=True)
        assert result.returncode == 0, f"pcsm version command failed: {result.stderr}"

        sbom_filename = f"percona-clustersync-mongodb-{PCSM_VER}.cdx.json"
        assert sbom_filename in contents, f"pcsm sbom not found in tarball. Found: {contents}"

        sbom_path = os.path.join(root, sbom_filename)
        result = subprocess.run(
            ["trivy", "sbom", "--severity", "HIGH,CRITICAL", "--ignore-unfixed", "--exit-code", "1", sbom_path],
            capture_output=True, text=True
        )
        assert result.returncode == 0, f"trivy sbom scan found HIGH/CRITICAL vulnerabilities:\n{result.stdout}\n{result.stderr}"

        cdx_result = subprocess.run(
            ["/usr/local/bin/cyclonedx", "validate", "--input-file", sbom_path, "--input-format", "json", "--input-version", "v1_6"],
            capture_output=True, text=True,
            env={**os.environ, "DOTNET_SYSTEM_GLOBALIZATION_INVARIANT": "1"}
        )
        assert cdx_result.returncode == 0, f"CycloneDX 1.6 schema validation failed: {cdx_result.stdout}\n{cdx_result.stderr}"
