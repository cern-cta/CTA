# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Implementation of the release tag command."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from commands import SubparserRegistry
from confirmation import confirm
from cta_version import CTAVersion, VersionError, select_development, select_release_candidate
from gitlab_api import GitLabAPIError
from release_config import ReleaseConfig, has_release_heading, validate_major_version
from release_context import ReleaseContext, ReleaseWorkflowError, info


@dataclass(frozen=True)
class TagPlan:
    """Fully preflighted tag publication state."""

    tag_name: str
    target_commit: str
    release_issue: dict[str, Any] | None
    description: str


def add_subparser(subparsers: SubparserRegistry) -> None:
    """Register the tag subcommand and its arguments."""
    parser = subparsers.add_parser(
        "tag",
        help="validate release context and create annotated release tags",
        description="Validate a release commit and publish one annotated CTA tag.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  release tag v6.12.0
  release tag v6.12.0.rc2
  release tag v6.12.0.dev1
  release tag v6.12.0 --development
  release tag v6.12.0 --release-candidate

One source tag starts the default builds; additional variants are manual.
Use --ref COMMIT_OR_TAG for corrected candidates or to promote a tested RC.
Use --dry-run before the subcommand to inspect the publication plan.""",
    )
    parser.add_argument("version")
    parser.add_argument(
        "--ref", help="commit or tag to release instead of the final changelog merge commit or prerelease branch tip"
    )
    parser.add_argument(
        "--target-branch",
        default=ReleaseConfig.default_branch,
        help="branch containing the release commit (default: %(default)s)",
    )
    parser.add_argument(
        "--yes", dest="skip_confirmation", action="store_true", help="create the tag without confirmation"
    )
    prerelease = parser.add_mutually_exclusive_group()
    prerelease.add_argument(
        "--development", action="store_true", help="automatically select the next devN for an unsuffixed version"
    )
    prerelease.add_argument(
        "--release-candidate", action="store_true", help="automatically select the next rcN for an unsuffixed version"
    )
    parser.set_defaults(execute=run_from_arguments)


def run_from_arguments(context: ReleaseContext, parsed_arguments: argparse.Namespace) -> None:
    """Translate parsed tag arguments into the typed command call."""
    run(
        context,
        parsed_arguments.version,
        parsed_arguments.skip_confirmation,
        parsed_arguments.target_branch,
        release_candidate=parsed_arguments.release_candidate,
        development=parsed_arguments.development,
        ref=parsed_arguments.ref,
    )


def edit_tag_description(context: ReleaseContext, version: str, target_commit: str) -> str:
    """Open Git's editor and require a non-empty annotated tag description."""
    editor = context.git.editor_command()
    template = (
        "\n"
        f"# Enter a short description for tag {version}.\n"
        f"# The tag will point to {target_commit}.\n"
        "# Lines starting with '#' are ignored.\n"
    )
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".txt", encoding="utf-8") as description_file:
        description_file.write(template)
        description_file.flush()
        editor_process = subprocess.run(  # noqa: S603
            [*shlex.split(editor), description_file.name],
            check=False,
        )
        if editor_process.returncode:
            raise ReleaseWorkflowError(f"Editor exited with status {editor_process.returncode}")
        description_file.seek(0)
        description = "\n".join(
            line for line in description_file.read().splitlines() if not line.lstrip().startswith("#")
        ).strip()
    if not description:
        raise ReleaseWorkflowError("Tag description is empty; the tag was not created")
    return description


def inspect_release_context(
    context: ReleaseContext,
    version_text: str,
    target_branch: str,
) -> tuple[dict[str, Any] | None, dict[str, Any], str, list[str]]:
    """Load the authoritative merged MR commit and advisory metadata warnings."""
    warnings: list[str] = []
    info(f"Looking for release ticket {context.config.issue_title(version_text)!r}")
    release_issue = context.find_or_create_release_issue(version_text, create=False)
    if release_issue is None:
        warnings.append(f"Release issue {context.config.issue_title(version_text)!r} was not found")

    info("Looking for the changelog merge request")
    changelog_merge_request = context.find_changelog_merge_request(version_text, target_branch)
    if changelog_merge_request is None:
        raise ReleaseWorkflowError(f"Release merge request for {version_text} was not found")
    if changelog_merge_request.get("state") != "merged":
        raise ReleaseWorkflowError(f"Release merge request {changelog_merge_request.get('web_url')} is not merged")

    target_commit = changelog_merge_request.get("merge_commit_sha") or changelog_merge_request.get("squash_commit_sha")
    if not target_commit:
        raise ReleaseWorkflowError(
            f"Release merge request {changelog_merge_request.get('web_url')} has no resulting commit"
        )

    try:
        info(f"Checking {context.config.changelog_file} at the selected commit for {version_text}")
        changelog, _ = context.read_repository_file(context.config.changelog_file, target_commit)
        if not has_release_heading(changelog, version_text):
            warnings.append(f"{context.config.changelog_file} at {target_commit} has no heading for {version_text}")
    except GitLabAPIError as error:
        warnings.append(f"Could not inspect {context.config.changelog_file} at {target_commit}: {error}")

    return release_issue, changelog_merge_request, str(target_commit), warnings


def _validate_tag(context: ReleaseContext, tag_name: str) -> None:
    """Require the tag to be absent locally and remotely."""
    if context.git.local_tag_commit(tag_name) or context.git.remote_tag_commit(context.config.remote, tag_name):
        raise ReleaseWorkflowError(f"Tag {tag_name} already exists; inspect the existing release")


def _find_tag_pipeline(context: ReleaseContext, target_commit: str, tag_name: str) -> dict[str, Any] | None:
    """Poll briefly for the tag's push pipeline."""
    info("Looking for the tag pipeline")
    for attempt in range(6):
        pipeline = context.find_pipeline(target_commit, tag_name, pipeline_source="push")
        if pipeline is not None:
            return pipeline
        if attempt < 5:
            time.sleep(2)
    return None


def _validate_release_metadata(
    context: ReleaseContext,
    version_text: str,
    branch_tip: str,
    skip_confirmation: bool,
    target_branch: str = ReleaseConfig.default_branch,
    ref: str | None = None,
    prerelease: bool = False,
) -> tuple[dict[str, Any] | None, str]:
    """Validate release metadata and the exact commit pipeline."""
    warnings: list[str] = []
    if prerelease:
        # Candidates can be tested before the final release changelog is prepared.
        info("Selecting prerelease commit without requiring release metadata")
        release_issue = context.find_or_create_release_issue(version_text, create=False)
        target_commit = branch_tip
        if ref is not None:
            target_commit = context.git.run(["rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"])
    else:
        info("Inspecting release metadata for the selected version and commit")
        release_issue, _, target_commit, warnings = inspect_release_context(context, version_text, target_branch)
        if ref is not None:
            selected_commit = context.git.run(["rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"])
            # Final releases must retain the reviewed release changelog.
            if not context.git.is_ancestor(target_commit, selected_commit):
                raise ReleaseWorkflowError("Selected revision must contain the merged release changelog")
            target_commit = selected_commit
            changelog, _ = context.read_repository_file(context.config.changelog_file, target_commit)
            if not has_release_heading(changelog, version_text):
                raise ReleaseWorkflowError("Selected revision no longer contains the release changelog entry")

    if not context.git.is_ancestor(target_commit, branch_tip):
        raise ReleaseWorkflowError(
            f"Release commit {target_commit} is not reachable from {context.config.remote}/{target_branch}"
        )

    validate_major_version(context.git, target_commit, version_text)

    if warnings:
        confirm(
            f"Continue preparing tag {version_text} despite these metadata warnings?",
            "Tag preparation declined; no changes were made",
            warnings=warnings,
            assume_yes=skip_confirmation,
            dry_run=context.dry_run,
        )
    elif not prerelease:
        info("Release issue, merged MR, and changelog entry were found")

    _validate_commit_pipeline(context, version_text, target_commit, target_branch, skip_confirmation)
    return release_issue, target_commit


def _validate_commit_pipeline(
    context: ReleaseContext,
    version_text: str,
    target_commit: str,
    target_branch: str,
    skip_confirmation: bool,
) -> None:
    """Check the selected commit's pipeline and confirm any pipeline warning."""
    # The pipeline gate protects the exact commit that will be tagged.
    info("Checking the pipeline for the selected commit")
    pipeline = context.find_pipeline(target_commit, target_branch, pipeline_source="push")
    if pipeline is None or pipeline.get("status") != "success":
        manual_pipeline = context.find_pipeline(target_commit, target_branch, pipeline_source="web")
        if manual_pipeline and (manual_pipeline.get("status") == "success" or pipeline is None):
            pipeline = manual_pipeline
    pipeline_status = "not found" if pipeline is None else str(pipeline.get("status") or "unknown")
    if context.config.require_successful_target_pipeline and pipeline_status != "success":
        pipeline_url = pipeline.get("web_url") if pipeline else None
        warning = f"Pipeline for release commit {target_commit} is {pipeline_status}"
        if pipeline_url:
            warning += f": {pipeline_url}"
        confirm(
            f"Continue preparing tag {version_text} without a successful pipeline?",
            "Tag preparation declined; no changes were made",
            warnings=[warning],
            assume_yes=skip_confirmation,
            dry_run=context.dry_run,
        )
    elif pipeline is None:
        info("No pipeline found; the pipeline gate is disabled")
    else:
        info(f"Found successful pipeline: {pipeline.get('web_url') or pipeline.get('id', 'unknown')}")


def build_tag_plan(
    context: ReleaseContext,
    tag_name: str,
    target_commit: str,
    release_issue: dict[str, Any] | None,
    skip_confirmation: bool,
) -> TagPlan:
    """Finish validation, editing, and confirmation before creating the tag."""
    print(f"Tag selected for publication: {tag_name}")
    _validate_tag(context, tag_name)
    if context.dry_run:
        info("DRY-RUN: would open Git's editor for the tag description")
        description = f"Release {tag_name}"
    else:
        info("Opening Git's editor for the annotated tag description")
        description = edit_tag_description(context, tag_name, target_commit)
    confirm(
        "Create and push the selected tag?",
        "Tag creation declined; no changes were made",
        assume_yes=skip_confirmation,
        dry_run=context.dry_run,
    )
    return TagPlan(tag_name, target_commit, release_issue, description)


def execute_tag_plan(context: ReleaseContext, plan: TagPlan) -> None:
    """Create and publish a fully preflighted tag."""
    info(f"Creating annotated tag {plan.tag_name}")
    context.git.create_tag(plan.target_commit, plan.tag_name, plan.description)
    info(f"Pushing {plan.tag_name} to GitLab")
    context.git.push_tag(context.config.remote, plan.tag_name)


def _report_tag_and_pipeline(context: ReleaseContext, plan: TagPlan) -> None:
    """Record publication and print the tag and pipeline links."""
    tag_name = plan.tag_name
    commit_url = f"{context.config.project_web_url}/-/commit/{plan.target_commit}"
    tag_url = f"{context.config.project_web_url}/-/tags/{quote(tag_name, safe='')}"
    print(f"Tag published successfully ({tag_name}): {tag_url}")
    print(
        "EL9/objectstore and EL9/pgsched with Oracle build automatically. "
        "Trigger any additional release variants manually from the tag pipeline's build jobs."
    )
    # Reporting failures must not obscure a successful push or suggest retrying the tag.
    try:
        context.add_issue_note(
            plan.release_issue,
            f"Tag `{tag_name}` created at commit [`{plan.target_commit[:12]}`]({commit_url}).",
        )
    except GitLabAPIError as error:
        print(f"WARNING: Tag is published, but its issue note could not be posted: {error}", file=sys.stderr)

    try:
        pipeline = _find_tag_pipeline(context, plan.target_commit, tag_name)
        if pipeline is None:
            print(f"Tag pipeline ({tag_name}) has not appeared yet; check GitLab shortly.")
        else:
            print(f"Tag pipeline ({tag_name}, {pipeline['status']}): {pipeline['web_url']}")
            context.add_issue_note(plan.release_issue, f"Tag pipeline ({tag_name}): {pipeline['web_url']}")
    except GitLabAPIError as error:
        print(f"WARNING: Tag is published, but pipeline reporting failed: {error}", file=sys.stderr)


def run(
    context: ReleaseContext,
    version_text: str,
    skip_confirmation: bool,
    target_branch: str = ReleaseConfig.default_branch,
    release_candidate: bool = False,
    ref: str | None = None,
    development: bool = False,
) -> None:
    """Resolve a revision, validate context, and publish a selected tag."""
    target_branch = target_branch or context.config.default_branch
    info(f"Using explicitly requested release version {version_text}")

    release_version = CTAVersion.parse(version_text)
    if release_candidate and development:
        raise VersionError("Choose either --release-candidate or --development")
    if (release_candidate or development) and (release_version.release_candidate or release_version.development):
        flag = "--development" if development else "--release-candidate"
        raise VersionError(f"{flag} requires an unsuffixed base version")
    # Release metadata belongs to the base, including for dev and RC tags.
    version_text = CTAVersion(*release_version.core).text
    remote_branch = f"{context.config.remote}/{target_branch}"
    info(f"Refreshing target branch {remote_branch!r}")
    branch_tip = context.git.resolve_remote_branch(
        context.config.remote,
        target_branch,
        fetch=not context.dry_run,
    )

    release_issue, target_commit = _validate_release_metadata(
        context,
        version_text,
        branch_tip,
        skip_confirmation,
        target_branch,
        ref,
        prerelease=bool(
            development or release_candidate or release_version.release_candidate or release_version.development
        ),
    )
    print(f"Target branch tip: {remote_branch} at {branch_tip}")
    print(f"Selected release commit to tag for {version_text}: {target_commit}")

    if release_candidate:
        release_version = release_version.with_components(
            release_candidate=select_release_candidate(release_version, context.git.tags())
        )
    elif development:
        release_version = release_version.with_components(
            development=select_development(release_version, context.git.tags())
        )
    plan = build_tag_plan(context, release_version.text, target_commit, release_issue, skip_confirmation)
    if context.dry_run:
        print(f"DRY-RUN: create and push tag: {plan.tag_name}")
        return

    execute_tag_plan(context, plan)
    _report_tag_and_pipeline(context, plan)
