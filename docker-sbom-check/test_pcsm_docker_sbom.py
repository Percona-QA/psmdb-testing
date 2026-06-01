import json
import os
import subprocess
import tempfile

PCSM_VER = os.environ.get("PCSM_VERSION")
IMAGE_REPO = os.environ.get("IMAGE_REPO", "perconalab")
PCSM_IMAGE = f"docker.io/{IMAGE_REPO}/percona-clustersync-mongodb:{PCSM_VER}"


def test_pcsm_docker_embedded_sbom():
    """Verify the SBOM embedded inside the Docker image exists and is CycloneDX 1.6 compliant"""
    sbom_filename = f"percona-clustersync-mongodb-{PCSM_VER}.cdx.json"
    sbom_path_in_image = f"/usr/share/doc/percona-clustersync-mongodb/{sbom_filename}"

    result = subprocess.run(
        ["docker", "run", "--rm", PCSM_IMAGE, "cat", sbom_path_in_image],
        capture_output=True,
    )
    assert result.returncode == 0, f"Embedded SBOM not found inside Docker image at {sbom_path_in_image}: {result.stderr.decode()}"

    with tempfile.TemporaryDirectory() as tmpdir:
        sbom_local = os.path.join(tmpdir, sbom_filename)
        with open(sbom_local, "wb") as f:
            f.write(result.stdout)
        trivy_result = subprocess.run(
            ["trivy", "sbom", "--severity", "HIGH,CRITICAL", "--ignore-unfixed", "--exit-code", "1", sbom_local],
            capture_output=True, text=True,
        )
        assert trivy_result.returncode == 0, (
            f"trivy sbom scan found HIGH/CRITICAL vulnerabilities:\n{trivy_result.stdout}\n{trivy_result.stderr}"
        )

        cdx_result = subprocess.run(
            ["/usr/local/bin/cyclonedx", "validate", "--input-file", sbom_local,
             "--input-format", "json", "--input-version", "v1_6"],
            capture_output=True, text=True,
            env={**os.environ, "DOTNET_SYSTEM_GLOBALIZATION_INVARIANT": "1"},
        )
        assert cdx_result.returncode == 0, (
            f"Embedded SBOM CycloneDX 1.6 validation failed: {cdx_result.stdout}\n{cdx_result.stderr}"
        )

def test_pcsm_docker_oci_sbom():
    """Verify the OCI-attached SBOM exists and is CycloneDX 1.6 compliant"""

    # oras discover needs a per-arch ref — resolve amd64 digest from the multi-arch index
    inspect_result = subprocess.run(
        ["docker", "manifest", "inspect", PCSM_IMAGE],
        capture_output=True, text=True,
    )
    assert inspect_result.returncode == 0, (
        f"docker manifest inspect failed: {inspect_result.stderr}"
    )

    manifest = json.loads(inspect_result.stdout)
    amd64_digest = next(
        (m["digest"] for m in manifest.get("manifests", [])
         if m.get("platform", {}).get("architecture") == "amd64"),
        None,
    )
    assert amd64_digest is not None, (
        f"Could not find amd64 manifest in image index. Manifests: {manifest.get('manifests')}"
    )

    image_base = PCSM_IMAGE.split(":")[0]
    amd64_ref = f"{image_base}@{amd64_digest}"

    trivy_result = subprocess.run(
        ["trivy", "image", "--severity", "HIGH,CRITICAL", "--ignore-unfixed",
         "--exit-code", "1", "--sbom-sources", "oci", PCSM_IMAGE],
        capture_output=True, text=True,
    )
    assert trivy_result.returncode == 0, (
        f"trivy image scan found HIGH/CRITICAL vulnerabilities:\n{trivy_result.stdout}\n{trivy_result.stderr}"
    )
    assert "Third-party SBOM" in trivy_result.stderr or "Third-party SBOM" in trivy_result.stdout, (
        f"OCI-attached SBOM was not found by trivy (expected 'Third-party SBOM' warning). "
        f"stderr: {trivy_result.stderr}"
    )

    # Pull SBOM via oras and validate format
    discover_result = subprocess.run(
        ["oras", "discover", "--format", "json", amd64_ref],
        capture_output=True, text=True,
    )
    assert discover_result.returncode == 0, (
        f"oras discover failed for {amd64_ref}: {discover_result.stderr}"
    )

    referrers_data = json.loads(discover_result.stdout)
    sbom_referrers = [
        r for r in referrers_data.get("referrers", [])
        if r.get("artifactType") == "application/vnd.cyclonedx+json"
    ]
    assert len(sbom_referrers) > 0, (
        f"No CycloneDX SBOM referrer found on {amd64_ref}. Got: {referrers_data}"
    )

    sbom_digest = sbom_referrers[0]["digest"]

    with tempfile.TemporaryDirectory() as tmpdir:
        pull_result = subprocess.run(
            ["oras", "pull", "--output", tmpdir, f"{image_base}@{sbom_digest}"],
            capture_output=True, text=True,
        )
        assert pull_result.returncode == 0, (
            f"oras pull of SBOM referrer failed: {pull_result.stderr}"
        )

        sbom_files = [f for f in os.listdir(tmpdir) if f.endswith(".cdx.json")]
        assert len(sbom_files) > 0, (
            f"No .cdx.json file found after oras pull. Files: {os.listdir(tmpdir)}"
        )

        sbom_path = os.path.join(tmpdir, sbom_files[0])
        cdx_result = subprocess.run(
            ["/usr/local/bin/cyclonedx", "validate", "--input-file", sbom_path,
             "--input-format", "json", "--input-version", "v1_6"],
            capture_output=True, text=True,
            env={**os.environ, "DOTNET_SYSTEM_GLOBALIZATION_INVARIANT": "1"},
        )
        assert cdx_result.returncode == 0, (
            f"OCI-attached SBOM CycloneDX 1.6 validation failed: {cdx_result.stdout}\n{cdx_result.stderr}"
        )
