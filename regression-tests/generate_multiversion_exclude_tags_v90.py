"""Run the release's native backport-tag generator without a Git checkout.

Build images omit .git. Read the old binary's exact public Percona commit
instead; retain the native version selection and backport comparison logic.
Run from the PSMDB source root with the same arguments as the resmoke
generate-multiversion-exclude-tags subcommand.
"""

import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path.cwd()))

from buildscripts.resmokelib import parser
from buildscripts.resmokelib.run import generate_multiversion_exclude_tags as generator


def get_public_git_file_content(commit_hash):
    """Fetch only the exact binary revision; never substitute a moving branch."""
    if not re.fullmatch(r"[0-9a-fA-F]{40}", commit_hash):
        raise ValueError(f"Expected an exact Git commit hash, got {commit_hash!r}")
    url = (
        "https://raw.githubusercontent.com/percona/percona-server-mongodb/"
        f"{commit_hash}/{generator.ETC_DIR}/{generator.BACKPORTS_REQUIRED_FILE}"
    )
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.text


if __name__ == "__main__":
    generator.get_git_file_content = get_public_git_file_content
    parser.parse_command_line(
        ["generate-multiversion-exclude-tags", *sys.argv[1:]],
        should_configure_otel=False,
    ).execute()
