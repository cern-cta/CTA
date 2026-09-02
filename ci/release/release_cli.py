# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Argument parsing and dispatch for the developer-facing release CLI."""

from __future__ import annotations

import argparse
import getpass
import os
import stat
import subprocess
import sys
from pathlib import Path

from commands import changelog, status, tag
from cta_version import VersionError
from git_repo import GitError, discover_repository_root
from gitlab_api import GitLabAPI, GitLabAPIError
from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError

# Default token file shared by various CTA CI scripts that need to make use of the GitLab API
TOKEN_FILE = Path.home() / ".config" / "cta" / "gitlab-api-token"


def load_token(dry_run: bool) -> str:
    """Load a GitLab token from the environment, shared file, or prompt."""
    token = os.environ.get("GITLAB_TOKEN")
    if token:
        return token
    if TOKEN_FILE.is_file():
        return TOKEN_FILE.read_text(encoding="utf-8").strip()

    # Token not found:
    if dry_run:
        raise ReleaseWorkflowError("Set GITLAB_TOKEN or create ~/.config/cta-ci-debug/token to run diagnostics")
    if not sys.stdin.isatty():
        raise ReleaseWorkflowError("GITLAB_TOKEN is required in a non-interactive terminal")

    print("GitLab authentication is required (token needs the api scope).")
    token = getpass.getpass("Token: ").strip()
    if not token:
        raise ReleaseWorkflowError("No GitLab token was provided")
    if input(f"Store token in {TOKEN_FILE}? [Y/n] ").strip().lower() in ("", "y", "yes"):
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(token, encoding="utf-8")
        TOKEN_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return token


def create_argument_parser() -> argparse.ArgumentParser:
    """Build the developer-facing release command parser."""
    argument_parser = argparse.ArgumentParser(
        prog="release",
        description="Create changelogs and tag CTA releases through GitLab.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Release workflow:
  1. Start from a clean, synchronized main checkout and generate the changelog:
       release changelog v5.12.0.0-1
  2. Review and merge the changelog MR printed by the command.
  3. Tag the merged release. With no version, the sole unfinished changelog MR is used:
       release tag

Common alternatives:
  Inspect progress without changing anything:
       release status v5.12.0.0-1
  Preview a command, including all validations and planned mutations:
       release --dry-run tag v5.12.0.0-1
Run "release COMMAND --help" for complete changelog, tag, and status scenarios.
Global options such as --dry-run must precede COMMAND.""",
    )

    # Global arguments
    argument_parser.add_argument("--dry-run", action="store_true", help="print planned mutations without making them")

    # For every command, add its parser
    command_subparsers = argument_parser.add_subparsers(required=True)
    changelog.add_subparser(command_subparsers)
    tag.add_subparser(command_subparsers)
    status.add_subparser(command_subparsers)

    return argument_parser


def main(argv: list[str] | None = None) -> int:
    """Run the requested release command and convert failures to exit codes."""
    parsed_arguments = create_argument_parser().parse_args(argv)

    try:
        repository_root = discover_repository_root()
        release_config = ReleaseConfig()
        gitlab_api = GitLabAPI(
            release_config.gitlab_url,
            release_config.project_id,
            load_token(parsed_arguments.dry_run),
        )
        release_context = ReleaseContext(
            repository_root,
            release_config,
            gitlab_api,
            parsed_arguments.dry_run,
        )

        parsed_arguments.execute(release_context, parsed_arguments)
    except (
        ReleaseWorkflowError,
        GitError,
        VersionError,
        GitLabAPIError,
        OSError,
        subprocess.CalledProcessError,
    ) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
