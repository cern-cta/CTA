# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Argument parsing and dispatch for the developer-facing release CLI."""

from __future__ import annotations

import argparse
import getpass
import os
import pwd
import stat
import subprocess
import sys
from pathlib import Path

from commands import changelog, status, tag
from confirmation import ConfirmationError, ask_yes_no
from cta_version import VersionError
from git_repo import GitError, discover_repository_root
from gitlab_api import GitLabAPI, GitLabAPIError
from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError

# Default token file shared by various CTA CI scripts that need to make use of the GitLab API
TOKEN_FILE = Path(pwd.getpwuid(os.getuid()).pw_dir) / ".config" / "cta" / "gitlab-api-token"


def load_token() -> tuple[str | None, str | None]:
    """Load a GitLab token and identify where it came from."""
    token = os.environ.get("GITLAB_TOKEN")
    if token:
        return token, "GITLAB_TOKEN"
    if TOKEN_FILE.is_file():
        return TOKEN_FILE.read_text(encoding="utf-8").strip(), str(TOKEN_FILE)
    return None, None


def prompt_for_token(gitlab_url: str, reason: str) -> str:
    """Explain how to create a token and securely read it from a terminal."""
    if not sys.stdin.isatty():
        raise ReleaseWorkflowError(f"{reason}; set GITLAB_TOKEN to a valid token with the api scope")

    print(f"GitLab authentication is required: {reason}.")
    print("Create a personal access token with the api scope here:")
    print(f"  {gitlab_url.rstrip('/')}/-/user_settings/personal_access_tokens")
    token = getpass.getpass("Token: ").strip()
    if not token:
        raise ReleaseWorkflowError("No GitLab token was provided")
    return token


def store_token(token: str) -> None:
    """Offer to persist a successfully authenticated token."""
    if ask_yes_no(f"Store token in {TOKEN_FILE}?", default_yes=True):
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(token, encoding="utf-8")
        TOKEN_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)


def create_authenticated_api(config: ReleaseConfig, dry_run: bool = False) -> GitLabAPI:
    """Load and validate credentials, prompting once for a replacement when needed."""
    token, token_source = load_token()
    prompted = token is None
    if token is None:
        token = prompt_for_token(config.gitlab_url, "no token was found")

    api = GitLabAPI(config.gitlab_url, config.project_id, token)
    try:
        api.authenticate()
    except GitLabAPIError as error:
        if error.status_code not in {401, 403}:
            raise
        source_description = f"the token from {token_source}" if token_source else "the supplied token"
        replacement = prompt_for_token(config.gitlab_url, f"{source_description} was rejected by GitLab")
        api = GitLabAPI(config.gitlab_url, config.project_id, replacement)
        api.authenticate()
        token = replacement
        prompted = True

    if prompted and not dry_run:
        store_token(token)
    return api


def create_argument_parser() -> argparse.ArgumentParser:
    """Build the developer-facing release command parser."""
    argument_parser = argparse.ArgumentParser(
        prog="release",
        description="Create changelogs and tag CTA releases through GitLab.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Release workflow:
  1. Synchronize the target branch and generate the changelog:
       release changelog v6.12.0.0-1
  2. Review and merge the changelog MR printed by the command.
  3. Tag the merged release:
       release tag v6.12.0.0-1
  For a release from another branch, pass the same target to each command:
       release changelog v6.12.0.0-1 --target-branch release/v6.11.23.0
       release tag v6.12.0.0-1 --target-branch release/v6.11.23.0

Common alternatives:
  Inspect progress without changing anything:
       release status v6.12.0.0-1
  Preview a command, including all validations and planned mutations:
       release --dry-run tag v6.12.0.0-1
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
        gitlab_api = create_authenticated_api(release_config, parsed_arguments.dry_run)
        release_context = ReleaseContext(
            repository_root,
            release_config,
            gitlab_api,
            parsed_arguments.dry_run,
        )

        parsed_arguments.execute(release_context, parsed_arguments)
    except (
        ReleaseWorkflowError,
        ConfirmationError,
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
