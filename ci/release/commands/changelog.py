# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Implementation of the release changelog command."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import tempfile
from typing import Any
from urllib.parse import quote

from commands import SubparserRegistry
from cta_version import CTAVersion, previous_release
from release_context import ReleaseContext, ReleaseWorkflowError, info

CHANGELOG_CATEGORIES = {
    "addition",
    "fix",
    "change",
    "deprecation",
    "removal",
    "security",
    "performance",
    "other",
}
MERGE_REQUEST_LABELS = ("type::release", "priority::high")


def add_subparser(subparsers: SubparserRegistry) -> None:
    """Register the changelog subcommand and its arguments."""
    parser = subparsers.add_parser(
        "changelog",
        help="create the release issue and changelog MR",
        description="Generate, review, and publish a changelog entry for an unsuffixed CTA release version.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""What this command does:
  1. Validates a clean main checkout, synchronizes it, and checks GitLab authentication.
  2. Finds or creates the release ticket.
  3. Generates changes since the previous numeric CTA release.
  4. Opens the generated entry in Git's configured editor.
  5. Creates or reuses the changelog branch and merge request.

Example:
  release changelog v5.12.0.0-1

The VERSION must be unsuffixed. Build variants and release candidates share the
base release's changelog and are selected later by "release tag".

After this command completes, review and merge the printed MR, then run:
  release tag

Use "release --dry-run changelog VERSION" to validate and preview the workflow
without opening an editor or changing GitLab.""",
    )
    parser.add_argument("version")
    parser.set_defaults(execute=run_from_arguments)


def run_from_arguments(context: ReleaseContext, parsed_arguments: argparse.Namespace) -> None:
    """Translate parsed changelog arguments into the typed command call."""
    run(context, parsed_arguments.version)


def merge_request_description(version: str, issue_iid: int) -> str:
    """Build the release changelog merge request description."""
    return (
        "### Description\n\n"
        f"Updates the changelog in preparation for release {version}.\n\n"
        f"See #{issue_iid}\n\n"
        "### Checklist\n\n"
        "- [x] Documentation reflects the changes made.\n"
        "- [x] Merge Request title is clear, concise, and suitable as a changelog entry. "
        "See [this link](https://cta.docs.cern.ch/latest/dev/contributing/workflow/#changelog)"
    )


def authenticated_user_id(authenticated_user: dict[str, Any]) -> int:
    """Return the validated numeric ID of the authenticated GitLab user."""
    user_id = authenticated_user.get("id")
    if type(user_id) is not int:
        raise ReleaseWorkflowError("Authenticated GitLab user has no valid numeric ID")
    return user_id


def escape_branch_search(value: str) -> str:
    """Escape CTA branch punctuation for GitLab's RE2 branch search."""
    return value.replace(".", r"\.")


def edit_changelog_notes(context: ReleaseContext, notes: str, version: str) -> str:
    """Open generated notes in Git's selected editor and validate them."""
    editor = context.git.editor_command()

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".md", encoding="utf-8") as candidate_file:
        candidate_file.write(notes)
        candidate_file.flush()
        editor_process = subprocess.run(  # noqa: S603
            [*shlex.split(editor), candidate_file.name],
            check=False,
        )
        if editor_process.returncode:
            raise ReleaseWorkflowError(f"Editor exited with status {editor_process.returncode}")
        candidate_file.seek(0)
        edited_notes = candidate_file.read().strip()

    heading_version = version.removeprefix("v")
    if not edited_notes or not any(
        line.startswith("## ") and heading_version in line for line in edited_notes.splitlines()
    ):
        raise ReleaseWorkflowError(f"Edited changelog must contain a level-two heading for {heading_version}")

    return edited_notes + "\n"


def confirm_changelog_publication() -> None:
    """Require explicit approval after the edited changelog has been reviewed."""
    answer = input("Continue and publish the edited changelog? [y/N] ").strip().lower()
    if answer not in ("y", "yes"):
        raise ReleaseWorkflowError("Changelog publication declined; no branch or merge request was created")


def generate_changelog_notes(
    context: ReleaseContext,
    previous_release: str,
    changelog_commit: str,
    version: str,
) -> str:
    """Generate notes and surface commits that lack valid changelog trailers."""
    response = context.api.get(
        "repository/changelog",
        {"from": previous_release, "to": changelog_commit, "version": version},
    )
    if not isinstance(response, dict) or not isinstance(response.get("notes"), str):
        raise ReleaseWorkflowError("GitLab returned an invalid repository/changelog response")

    commits_in_range = context.api.get_all(
        "repository/commits",
        {"ref_name": f"{previous_release}..{changelog_commit}", "trailers": True},
    )
    commits_missing_valid_trailers = [
        commit for commit in commits_in_range if commit.get("trailers", {}).get("Changelog") not in CHANGELOG_CATEGORIES
    ]
    changelog_notes = response["notes"].rstrip()

    if commits_missing_valid_trailers:
        missing_trailer_entries = "\n".join(
            f"- [{commit['title']}]({commit['web_url']})" for commit in commits_missing_valid_trailers
        )
        changelog_notes += (
            "\n\n### Commits missing valid Changelog trailers\n\n"
            "<!-- Remove these entries or move them to the correct category before saving. -->\n\n"
            f"{missing_trailer_entries}"
        )

    return changelog_notes + "\n"


def changelog_issue_note(
    context: ReleaseContext,
    version: str,
    changelog_commit: str,
    changelog_branch: str,
    branch_url: str,
    changelog_merge_request: dict[str, Any],
) -> str:
    """Build the single issue comment summarizing changelog preparation."""
    commit_url = f"{context.config.project_web_url}/-/commit/{changelog_commit}"
    return "\n".join(
        [
            f"The commit produced by merging "
            f"[merge request !{changelog_merge_request['iid']}]({changelog_merge_request['web_url']}) "
            f"will be tagged as **{version}**.",
            "",
            f"The changelog covers changes through commit [`{changelog_commit[:12]}`]({commit_url}) and was "
            f"generated on branch [`{changelog_branch}`]({branch_url}).",
            "",
            "Awaiting MR approval and merge.",
        ]
    )


def _validate_tag_is_available(context: ReleaseContext, release_version: CTAVersion) -> None:
    """Ensure the exact release tag does not already exist."""
    if context.git.local_tag_commit(release_version.text) or context.git.remote_tag_commit(
        context.config.remote,
        release_version.text,
    ):
        raise ReleaseWorkflowError(
            f"Tag {release_version.text} already exists; use release status {release_version.text}"
        )


def _publish_changelog(
    context: ReleaseContext,
    release_version: CTAVersion,
    changelog_commit: str,
    generated_notes: str,
    user_id: int,
    release_issue: dict[str, Any],
    existing_merge_request: dict[str, Any] | None,
) -> None:
    """Publish edited notes to a branch and create or reuse its merge request."""
    # Let the developer review the generated content.
    info("Opening the generated changelog in Git's editor")
    edited_notes = edit_changelog_notes(context, generated_notes, release_version.text)
    confirm_changelog_publication()
    changelog_branch = context.config.changelog_branch(release_version.text)

    # Create or validate the deterministic changelog branch.
    info(f"Creating or verifying changelog branch {changelog_branch}")
    branches = context.api.get_all(
        "repository/branches",
        {"search": f"^{escape_branch_search(changelog_branch)}$"},
    )
    exact_branches = [item for item in branches if item.get("name") == changelog_branch]
    if exact_branches:
        branch_resource = exact_branches[0]
        if exact_branches[0]["commit"]["id"] != changelog_commit and existing_merge_request is None:
            raise ReleaseWorkflowError(
                f"Existing branch {changelog_branch} does not start at expected commit {changelog_commit}"
            )
    else:
        branch_resource = context.api.post(
            "repository/branches",
            params={"branch": changelog_branch, "ref": changelog_commit},
        )

    # Add the approved entry to the branch changelog.
    info(f"Updating {context.config.changelog_file} on {changelog_branch}")
    old_changelog, _ = context.read_repository_file(context.config.changelog_file, changelog_branch)
    heading_version = release_version.text.removeprefix("v")
    has_release_heading = any(line.startswith("## ") and heading_version in line for line in old_changelog.splitlines())
    if has_release_heading:
        print(f"Reusing existing changelog entry for {release_version.text} on {changelog_branch}.")
    elif existing_merge_request is not None:
        raise ReleaseWorkflowError(
            f"Existing changelog MR {existing_merge_request['web_url']} does not contain the expected "
            f"{release_version.text} changelog heading; inspect it before rerunning changelog"
        )
    else:
        context.api.put(
            f"repository/files/{quote(context.config.changelog_file, safe='')}",
            json={
                "branch": changelog_branch,
                "content": edited_notes + "\n" + old_changelog.lstrip(),
                "commit_message": f"[Misc] Update changelog for release {heading_version}",
            },
        )

    # Create the reviewable release merge request.
    info("Creating or reusing the changelog merge request")
    changelog_merge_request = context.find_changelog_merge_request(release_version.text)
    if changelog_merge_request is None:
        changelog_merge_request = context.api.post(
            "merge_requests",
            json={
                "source_branch": changelog_branch,
                "target_branch": context.config.default_branch,
                "title": context.config.changelog_merge_request_title(release_version.text),
                "description": merge_request_description(release_version.text, release_issue["iid"]),
                "labels": ",".join(MERGE_REQUEST_LABELS),
                "assignee_ids": [user_id],
                "reviewer_ids": [user_id],
                "remove_source_branch": True,
                "squash": True,
            },
        )

    branch_url = branch_resource.get(
        "web_url",
        f"{context.config.project_web_url}/-/tree/{quote(changelog_branch, safe='')}",
    )
    context.add_issue_note(
        release_issue,
        release_version.text,
        "changelog",
        changelog_issue_note(
            context,
            release_version.text,
            changelog_commit,
            changelog_branch,
            branch_url,
            changelog_merge_request,
        ),
    )
    print(
        f"\nChangelog for {release_version.text} prepared."
        f"\n\nRelease ticket:\n{release_issue['web_url']}"
        f"\n\nReview and merge:\n{changelog_merge_request['web_url']}"
        "\n\nAfter it is merged, run:\nrelease tag"
    )


def run(context: ReleaseContext, version_text: str) -> None:
    """Create or reuse the issue, changelog branch, and merge request."""
    release_version = CTAVersion.parse(version_text, require_base=True)

    # Validate local and remote release prerequisites.
    info("Validating the local repository")
    changelog_commit = context.git.validate_repository(
        context.config.default_branch,
        context.config.remote,
        fetch=not context.dry_run,
    )
    info("Checking GitLab authentication")
    user_id = authenticated_user_id(context.api.authenticate())

    # Reuse safe release resources and reject conflicting state.
    info(f"Checking release state for {release_version.text}")
    existing_merge_request = context.find_changelog_merge_request(release_version.text)
    if existing_merge_request and existing_merge_request.get("state") == "merged":
        print(f"{release_version.text} already has a merged changelog MR. Run: release tag {release_version.text}")
        return

    _validate_tag_is_available(context, release_version)
    release_issue = context.find_or_create_release_issue(release_version.text, create=True)
    if release_issue is not None:
        info(f"Release ticket: {release_issue['web_url']}")

    previous_version = previous_release(release_version, context.git.tags())

    # Generate the candidate changelog from the previous numeric release.
    info(f"Generating changelog from {previous_version.text} through {changelog_commit}")
    generated_notes = generate_changelog_notes(
        context,
        previous_version.text,
        changelog_commit,
        release_version.text.removeprefix("v"),
    )

    if context.dry_run:
        print(f"DRY-RUN: open generated changelog for {release_version.text} in Git's editor")
        print(
            f"DRY-RUN: create or verify branch {context.config.changelog_branch(release_version.text)} "
            f"at {changelog_commit}"
        )
        print(f"DRY-RUN: update {context.config.changelog_file} and create or reuse its merge request")
        return

    if release_issue is None:
        raise ReleaseWorkflowError("Release issue creation returned no issue; cannot create a linked merge request")

    _publish_changelog(
        context,
        release_version,
        changelog_commit,
        generated_notes,
        user_id,
        release_issue,
        existing_merge_request,
    )
