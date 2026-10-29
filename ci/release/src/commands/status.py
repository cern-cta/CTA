# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Implementation of the read-only release status command."""

from __future__ import annotations

import argparse
from contextlib import suppress

from cta_version import CTAVersion, VersionError

from commands import SubparserRegistry
from release_config import ReleaseConfig
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
    release status v6.12.0
    release status v6.12.0 --target-branch maintenance

This command is always read-only.""",
    )
    parser.add_argument("version")
    parser.add_argument(
        "--target-branch",
        default=ReleaseConfig.default_branch,
        help="branch containing the release changelog (default: %(default)s)",
    )
    parser.set_defaults(execute=run_from_arguments)


def run_from_arguments(context: ReleaseContext, parsed_arguments: argparse.Namespace) -> None:
    """Translate parsed status arguments into the typed command call."""
    run(context, parsed_arguments.version, parsed_arguments.target_branch)


def run(context: ReleaseContext, version_text: str, target_branch: str = ReleaseConfig.default_branch) -> None:
    """Print reconstructed Git and GitLab state for a release."""
    context.git.validate_target_branch(target_branch)
    version = CTAVersion.parse(version_text)
    base = CTAVersion(*version.core).text
    info(f"Inspecting release status for {version_text}")
    issue = context.find_or_create_release_issue(base, create=False)
    mr = context.find_changelog_merge_request(base, target_branch)
    print(f"Version:  {version_text}")
    print(f"Issue:    {issue['web_url'] if issue else 'not created'}")
    print(f"MR:       {mr['web_url']} ({mr['state']})" if mr else "MR:       not created")
    if mr and mr.get("state") == "merged":
        commit = mr.get("merge_commit_sha") or mr.get("squash_commit_sha")
        print(f"Changelog commit: {commit or 'not available'}")

    # Query remote tags so status also works without fetching newly published candidates.
    tags = context.api.get_all("repository/tags", {"search": f"^{base}"})
    matching_tags = []
    for tag in tags:
        with suppress(VersionError):
            candidate = CTAVersion.parse(tag["name"])
            if candidate.core == version.core and version.text in (base, candidate.text):
                matching_tags.append(tag)
    if not matching_tags:
        print("Tags:     not published")
    for tag in matching_tags:
        commit = tag["commit"]["id"]
        print(f"Tag:      {tag['name']} at {commit}")
        pipeline = context.find_pipeline(commit, tag["name"], pipeline_source="push")
        if pipeline is None:
            print("  Tag pipeline: not found")
            continue
        print(f"  Tag pipeline: {pipeline['status']} {pipeline.get('web_url', '')}")
        # Include optional triggers: a successful parent does not mean every variant ran.
        for bridge in context.api.get_all(f"pipelines/{pipeline['id']}/bridges"):
            child = bridge.get("downstream_pipeline")
            state = child["status"] if child else bridge["status"]
            url = child.get("web_url", "") if child else bridge.get("web_url", "")
            print(f"    {bridge['name']}: {state} {url}")
