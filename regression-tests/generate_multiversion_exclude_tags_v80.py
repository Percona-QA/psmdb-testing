"""Generate v8.0 last-LTS backport exclusions without a Git checkout.

Use the current PSMDB checkout's native comparison, including its handling of
flat and version-keyed backport files. Retrieve historical metadata only from
the exact public Percona revision reported by mongod-last-lts --version.
PSMDB 8.0 regression does not provision last-continuous binaries.
"""

import argparse
import re
import sys
from pathlib import Path

import requests


def get_public_git_file_content(commit_hash):
    """Fail rather than substitute a moving branch for an unknown binary revision."""
    if not re.fullmatch(r"[0-9a-fA-F]{40}", commit_hash):
        raise ValueError(f"Expected an exact old-binary commit hash, got {commit_hash!r}")
    url = (
        "https://raw.githubusercontent.com/percona/percona-server-mongodb/"
        f"{commit_hash}/etc/backports_required_for_multiversion_tests.yml"
    )
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.text


def main(argv=None):
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--oldBinVersion", choices=["last_lts"], default="last_lts")
    cli.add_argument("--excludeTagsFilePath", required=True)
    cli.add_argument("--multiversionDir", action="append", default=[])
    cli.add_argument(
        "--extraExcludeTest", action="append", default=[],
        help="Merge an explicitly selected test into the generated backport tag file.",
    )
    args = cli.parse_args(argv)
    for test in args.extraExcludeTest:
        if not Path(test).is_file():
            cli.error(f"Extra exclusion does not exist in the current checkout: {test}")

    # The release checkout, not this wrapper, owns FCV/binary selection and the
    # backport comparison. Only replace its Git-based historical file reader.
    sys.path.insert(0, str(Path.cwd()))
    from buildscripts.resmokelib import config, parser
    from buildscripts.resmokelib.run import generate_multiversion_exclude_tags as generator
    from buildscripts.resmokelib.testing.tags import TagsConfig

    generator.get_git_file_content = get_public_git_file_content
    native_args = [
        "generate-multiversion-exclude-tags",
        f"--oldBinVersion={args.oldBinVersion}",
        f"--excludeTagsFilePath={args.excludeTagsFilePath}",
    ]
    # v8.0 has no --multiversionDir option for this subcommand. Its binary resolver
    # reads DEFAULT_MULTIVERSION_DIRS through get_path_env_var().
    config.DEFAULT_MULTIVERSION_DIRS = args.multiversionDir + [
        directory for directory in config.DEFAULT_MULTIVERSION_DIRS
        if directory not in args.multiversionDir
    ]
    parser.parse_command_line(native_args).execute()

    # Merge structurally: native output may be `selector: {}`, so appending YAML
    # lines would corrupt an empty output or duplicate an existing mapping key.
    if args.extraExcludeTest:
        tags = TagsConfig.from_file(args.excludeTagsFilePath)
        for test in args.extraExcludeTest:
            tags.add_tag("js_test", test, generator.BACKPORT_REQUIRED_TAG)
        tags.write_file(
            args.excludeTagsFilePath,
            preamble="Native v8.0 last-LTS backport tags with explicit regression exclusions.",
        )


if __name__ == "__main__":
    main()
