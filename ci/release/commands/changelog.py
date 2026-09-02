# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Implementation of the release changelog command."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from commands import SubparserRegistry
from confirmation import confirm
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


@dataclass(frozen=True)
class ChangelogPlan:
    """Fully preflighted changelog publication state."""

    release_version: CTAVersion
    target_branch: str
    changelog_commit: str
    edited_notes: str
    user_id: int
    release_issue: dict[str, Any] | None
    merge_request: dict[str, Any] | None
    branch_resource: dict[str, Any] | None
    old_changelog: str
    has_release_heading: bool


def add_subparser(subparsers: SubparserRegistry) -> None:
    """Register the changelog subcommand and its arguments."""
    parser = subparsers.add_parser(
        "changelog",
        help="create the release issue and changelog MR",
        description="Generate, review, and publish a changelog entry for an unsuffixed CTA release version.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""What this command does:
  1. Validates and synchronizes the selected target branch, then checks GitLab authentication.
  2. Generates changes since the previous numeric CTA release.
  3. Opens the generated entry in Git's configured editor for approval.
  4. Finds or creates the release ticket, changelog branch, and merge request.

Example:
  release changelog v5.12.0.0-1
  release changelog v5.12.0.0-1 --target-branch maintenance

The VERSION must be unsuffixed. Build variants and release candidates share the
base release's changelog and are selected later by "release tag".

After this command completes, review and merge the printed MR, then run release
tag with the same VERSION and --target-branch selection.

Use "release --dry-run changelog VERSION" to validate and preview the workflow
without opening an editor or changing GitLab.""",
    )
    parser.add_argument("version")
    parser.add_argument(
        "--target-branch",
        default="main",
        help="branch to prepare and merge the changelog into (default: main)",
    )
    parser.set_defaults(execute=run_from_arguments)


def run_from_arguments(context: ReleaseContext, parsed_arguments: argparse.Namespace) -> None:
    """Translate parsed changelog arguments into the typed command call."""
    run(context, parsed_arguments.version, parsed_arguments.target_branch)


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


def confirm_changelog_publication(dry_run: bool = False) -> None:
    """Require explicit approval after the edited changelog has been reviewed."""
    confirm(
        "Continue and publish the edited changelog?",
        "Changelog publication declined; no branch or merge request was created",
        dry_run=dry_run,
    )


def confirm_changelog_reuse(
    changelog_branch: str,
    branch_resource: dict[str, Any] | None,
    merge_request: dict[str, Any] | None,
    dry_run: bool = False,
) -> None:
    """Require approval before reusing an existing changelog branch or open MR."""
    resources = []
    if branch_resource is not None:
        branch_commit = branch_resource.get("commit", {}).get("id", "unknown")
        resources.append(f"branch {changelog_branch} at {branch_commit}")
    if merge_request is not None and merge_request.get("state") == "opened":
        resources.append(f"open merge request {merge_request.get('web_url', 'with unknown URL')}")
    if not resources:
        return

    confirm(
        "Continue and reuse these changelog resources?",
        "Changelog resource reuse declined; no release issue was created",
        warnings=[f"Existing changelog resource will be reused: {resource}" for resource in resources],
        dry_run=dry_run,
    )


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


def build_changelog_plan(
    context: ReleaseContext,
    release_version: CTAVersion,
    changelog_commit: str,
    generated_notes: str,
    user_id: int,
    existing_merge_request: dict[str, Any] | None,
    target_branch: str,
) -> ChangelogPlan:
    """Finish all remote reads, editing, validation, and confirmation."""
    if context.dry_run:
        info("DRY-RUN: would open the generated changelog in Git's editor")
        edited_notes = generated_notes
    else:
        info("Opening the generated changelog in Git's editor")
        edited_notes = edit_changelog_notes(context, generated_notes, release_version.text)
    confirm_changelog_publication(context.dry_run)
    changelog_branch = context.config.changelog_branch(release_version.text, target_branch)

    info(f"Inspecting changelog branch {changelog_branch}")
    branches = context.api.get_all(
        "repository/branches",
        {"search": f"^{escape_branch_search(changelog_branch)}$"},
    )
    exact_branches = [item for item in branches if item.get("name") == changelog_branch]
    branch_resource = exact_branches[0] if exact_branches else None
    if exact_branches and exact_branches[0]["commit"]["id"] != changelog_commit and existing_merge_request is None:
        raise ReleaseWorkflowError(
            f"Existing branch {changelog_branch} does not start at expected commit {changelog_commit}"
        )

    content_ref = changelog_branch if branch_resource is not None else changelog_commit
    old_changelog, _ = context.read_repository_file(context.config.changelog_file, content_ref)
    heading_version = release_version.text.removeprefix("v")
    has_release_heading = any(line.startswith("## ") and heading_version in line for line in old_changelog.splitlines())
    if existing_merge_request is not None and not has_release_heading:
        raise ReleaseWorkflowError(
            f"Existing changelog MR {existing_merge_request['web_url']} does not contain the expected "
            f"{release_version.text} changelog heading; inspect it before rerunning changelog"
        )

    confirm_changelog_reuse(
        changelog_branch,
        branch_resource,
        existing_merge_request,
        context.dry_run,
    )
    release_issue = context.find_or_create_release_issue(release_version.text, create=False)
    return ChangelogPlan(
        release_version=release_version,
        target_branch=target_branch,
        changelog_commit=changelog_commit,
        edited_notes=edited_notes,
        user_id=user_id,
        release_issue=release_issue,
        merge_request=existing_merge_request,
        branch_resource=branch_resource,
        old_changelog=old_changelog,
        has_release_heading=has_release_heading,
    )


def execute_changelog_plan(context: ReleaseContext, plan: ChangelogPlan) -> None:
    """Apply a fully preflighted changelog plan without further discovery reads."""
    version = plan.release_version.text
    changelog_branch = context.config.changelog_branch(version, plan.target_branch)
    release_issue = plan.release_issue or context.create_release_issue(version)
    info(f"Release ticket: {release_issue['web_url']}")

    branch_resource = plan.branch_resource
    if branch_resource is None:
        branch_resource = context.api.post(
            "repository/branches",
            params={"branch": changelog_branch, "ref": plan.changelog_commit},
        )

    if plan.has_release_heading:
        print(f"Reusing existing changelog entry for {version} on {changelog_branch}.")
    else:
        heading_version = version.removeprefix("v")
        context.api.put(
            f"repository/files/{quote(context.config.changelog_file, safe='')}",
            json={
                "branch": changelog_branch,
                "content": plan.edited_notes + "\n" + plan.old_changelog.lstrip(),
                "commit_message": f"[Misc] Update changelog for release {heading_version}",
            },
        )

    info("Creating or reusing the changelog merge request")
    changelog_merge_request = plan.merge_request
    if changelog_merge_request is None:
        changelog_merge_request = context.api.post(
            "merge_requests",
            json={
                "source_branch": changelog_branch,
                "target_branch": plan.target_branch,
                "title": context.config.changelog_merge_request_title(version),
                "description": merge_request_description(version, release_issue["iid"]),
                "labels": ",".join(MERGE_REQUEST_LABELS),
                "assignee_ids": [plan.user_id],
                "reviewer_ids": [plan.user_id],
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
        version,
        "changelog",
        changelog_issue_note(
            context,
            version,
            plan.changelog_commit,
            changelog_branch,
            branch_url,
            changelog_merge_request,
        ),
    )
    print(
        f"\nChangelog for {version} prepared."
        f"\n\nRelease ticket:\n{release_issue['web_url']}"
        f"\n\nReview and merge:\n{changelog_merge_request['web_url']}"
        f"\n\nAfter it is merged, run:\nrelease tag {version}"
        + (f" --target-branch {plan.target_branch}" if plan.target_branch != context.config.default_branch else "")
    )


def run(context: ReleaseContext, version_text: str, target_branch: str = "main") -> None:
    """Create or reuse the issue, changelog branch, and merge request."""
    release_version = CTAVersion.parse(version_text, require_base=True)

    # Validate local and remote release prerequisites.
    info("Validating the local repository")
    changelog_commit = context.git.validate_repository(
        target_branch,
        context.config.remote,
        fetch=not context.dry_run,
    )
    info("Checking GitLab authentication")
    user_id = authenticated_user_id(context.api.authenticate())

    # Reuse safe release resources and reject conflicting state.
    info(f"Checking release state for {release_version.text}")
    existing_merge_request = context.find_changelog_merge_request(release_version.text, target_branch)
    if existing_merge_request and existing_merge_request.get("state") == "merged":
        command = f"release tag {release_version.text}"
        if target_branch != context.config.default_branch:
            command += f" --target-branch {target_branch}"
        print(f"{release_version.text} already has a merged changelog MR. Run: {command}")
        return

    _validate_tag_is_available(context, release_version)
    previous_version = previous_release(release_version, context.git.tags())

    # Generate the candidate changelog from the previous numeric release.
    info(f"Generating changelog from {previous_version.text} through {changelog_commit}")
    generated_notes = generate_changelog_notes(
        context,
        previous_version.text,
        changelog_commit,
        release_version.text.removeprefix("v"),
    )

    plan = build_changelog_plan(
        context,
        release_version,
        changelog_commit,
        generated_notes,
        user_id,
        existing_merge_request,
        target_branch,
    )
    if context.dry_run:
        changelog_branch = context.config.changelog_branch(release_version.text, target_branch)
        print(f"DRY-RUN: create or verify branch {changelog_branch} at {changelog_commit}")
        print(f"DRY-RUN: update {context.config.changelog_file} and create or reuse its merge request")
        return
    execute_changelog_plan(context, plan)
