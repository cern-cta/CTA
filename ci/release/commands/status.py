# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Implementation of the read-only release status command."""

from __future__ import annotations

import argparse

from commands import SubparserRegistry
from release_context import ReleaseContext, info


def add_subparser(subparsers: SubparserRegistry) -> None:
    """Register the status subcommand and its arguments."""
    parser = subparsers.add_parser(
        "status",
        help="show reconstructed release state",
        description="Inspect release issue, changelog MR, commit, pipeline, and tag state without making changes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  Inspect a known release:
    release status v5.12.0.0-1

  Discover the sole merged changelog MR whose base tag is unfinished:
    release status

This command is always read-only. Discovery fails rather than guessing when no
unfinished release or more than one unfinished release is found.""",
    )
    parser.add_argument("version", nargs="?")
    parser.set_defaults(execute=run_from_arguments)


def run_from_arguments(context: ReleaseContext, parsed_arguments: argparse.Namespace) -> None:
    """Translate parsed status arguments into the typed command call."""
    run(context, parsed_arguments.version)


def run(context: ReleaseContext, version_text: str | None) -> None:
    """Print reconstructed Git and GitLab state for a release."""
    version_text = version_text or context.discover_unfinished_release_version()
    info(f"Inspecting release status for {version_text}")
    release_issue, changelog_merge_request, release_commit = context.load_release_context(
        version_text, validate_local=False
    )
    pipeline = context.find_pipeline(release_commit, context.config.default_branch, pipeline_source="push")
    local_tag_commit = context.git.local_tag_commit(version_text)
    remote_tag_commit = context.git.remote_tag_commit(context.config.remote, version_text)

    print(f"Version:  {version_text}")
    print(f"Issue:    {release_issue['web_url']}")
    print(f"MR:       {changelog_merge_request['web_url']} ({changelog_merge_request['state']})")
    print(f"Commit:   {release_commit}")
    print(f"Pipeline: {pipeline['status'] if pipeline else 'not found'}")
    print(f"Tag:      local={local_tag_commit or 'absent'}, remote={remote_tag_commit or 'absent'}")
