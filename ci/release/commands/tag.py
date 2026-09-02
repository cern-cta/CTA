# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Implementation of the release tag command."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import tempfile
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from commands import SubparserRegistry
from confirmation import ask_yes_no, confirm
from cta_version import (
    BUILD_VARIANTS,
    CTAVersion,
    BuildVariant,
    parse_build_variants,
    select_release_candidate,
)
from gitlab_api import GitLabAPIError
from release_context import ReleaseContext, ReleaseWorkflowError, info


@dataclass(frozen=True)
class TagPlan:
    """Fully preflighted tag publication state."""

    version_text: str
    target_branch: str
    target_commit: str
    release_issue: dict[str, Any] | None
    tag_descriptions: dict[str, str]


def add_subparser(subparsers: SubparserRegistry) -> None:
    """Register the tag subcommand and its arguments."""
    parser = subparsers.add_parser(
        "tag",
        help="validate release context and create annotated release tags",
        description="Validate a release commit and publish one or more annotated CTA tags.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Typical final release:
  release tag v5.12.0.0-1
    Prompts whether to create the base tag plus pgsched, pgcat, and pgall.

Unattended final release:
  release tag v5.12.0.0-1 --yes
    Creates the base tag and all three build variants without confirmation.

Only selected build variants (no base tag):
  release tag v5.12.0.0-1 --suffix pgsched --suffix pgcat
    --suffix may be repeated; duplicates are removed and canonical order is used.

Automatically numbered release candidate:
  release tag v5.12.0.0-1 --release-candidate
  release tag v5.12.0.0-1 --release-candidate --suffix pgall
    Existing RC families are inspected and the next unused rcN is selected.

Tag a release merged into another target branch:
  release tag v5.12.0.0-1 --target-branch maintenance
    The merged changelog MR commit is tagged after verifying it is on origin/maintenance.
    The default target branch is main.

Before publishing, the command prints the exact commit, checks release metadata,
requires a successful push pipeline for that commit, opens Git's editor for one
shared tag description, and verifies every selected tag is absent locally and
remotely. The complete new tag family is pushed atomically.

Use "release --dry-run tag VERSION" to perform preflight and validation without
opening an editor, creating tags, pushing refs, or changing GitLab.""",
    )
    parser.add_argument("version")
    parser.add_argument(
        "--target-branch",
        default="main",
        help="branch containing the merged changelog and release commit (default: main)",
    )
    parser.add_argument(
        "--yes",
        dest="skip_confirmation",
        action="store_true",
        help="create the tag without confirmation",
    )
    parser.add_argument(
        "--release-candidate",
        action="store_true",
        help="automatically select and create the next RC tag family",
    )
    parser.add_argument(
        "--suffix",
        dest="requested_suffixes",
        action="append",
        choices=[variant.value for variant in BUILD_VARIANTS],
        default=[],
        help="create only this PostgreSQL variant; may be repeated",
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
        requested_suffixes=parsed_arguments.requested_suffixes,
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

    target_commit = changelog_merge_request.get("squash_commit_sha") or changelog_merge_request.get("merge_commit_sha")
    if not target_commit:
        raise ReleaseWorkflowError(
            f"Release merge request {changelog_merge_request.get('web_url')} has no resulting commit"
        )

    try:
        info(f"Checking {context.config.changelog_file} at the selected commit for {version_text}")
        changelog, _ = context.read_repository_file(context.config.changelog_file, target_commit)
        heading_version = version_text.removeprefix("v")
        if not any(line.startswith("## ") and heading_version in line for line in changelog.splitlines()):
            warnings.append(f"{context.config.changelog_file} at {target_commit} has no heading for {version_text}")
    except GitLabAPIError as error:
        warnings.append(f"Could not inspect {context.config.changelog_file} at {target_commit}: {error}")

    return release_issue, changelog_merge_request, str(target_commit), warnings


def build_tag_description(shared_description: str, variant: BuildVariant | None) -> str:
    """Append the selected build-variant explanation to a shared tag message."""
    if variant is None:
        return shared_description
    return f"{shared_description.rstrip()}\n\n{variant.description}"


def _select_build_variants(
    context: ReleaseContext,
    skip_confirmation: bool,
    requested_suffixes: list[str] | None,
) -> tuple[bool, tuple[BuildVariant, ...]]:
    """Resolve explicit or interactively selected build variants."""
    variants_explicitly_selected = bool(requested_suffixes)
    build_variants = parse_build_variants(requested_suffixes or [])

    if variants_explicitly_selected:
        return True, build_variants

    include_variants = ask_yes_no(
        "Also create pgsched, pgcat, and pgall tag variants?",
        assume_yes=skip_confirmation,
        dry_run=context.dry_run,
    )
    return False, BUILD_VARIANTS if include_variants else build_variants


def _validate_selected_tags(
    context: ReleaseContext,
    tag_names: list[str],
    target_commit: str,
) -> None:
    """Require every tag in a new family to be absent locally and remotely."""
    del target_commit
    local_tags = {name: context.git.local_tag_commit(name) for name in tag_names}
    remote_tags = context.git.remote_tag_commits(context.config.remote, tag_names)
    existing = [name for name in tag_names if local_tags[name] is not None or name in remote_tags]
    if existing:
        raise ReleaseWorkflowError(
            f"Tag family already exists in part or in full: {', '.join(existing)}; "
            "remove conflicting local state or inspect the existing release"
        )


def _find_tag_pipelines(
    context: ReleaseContext,
    target_commit: str,
    tag_names: list[str],
) -> dict[str, dict[str, Any]]:
    """Poll briefly for the push pipeline belonging to every selected tag."""
    pipelines: dict[str, dict[str, Any]] = {}
    if context.dry_run:
        return pipelines

    info("Looking for tag pipelines")
    for _ in range(6):
        for tag_name in tag_names:
            if tag_name in pipelines:
                continue

            pipeline = context.find_pipeline(target_commit, tag_name, pipeline_source="push")
            if pipeline is not None:
                pipelines[tag_name] = pipeline

        if len(pipelines) == len(tag_names):
            break
        time.sleep(2)

    return pipelines


def _validate_release_metadata(
    context: ReleaseContext,
    version_text: str,
    branch_tip: str,
    skip_confirmation: bool,
    target_branch: str = "main",
) -> tuple[dict[str, Any] | None, str]:
    """Validate release metadata and the exact commit pipeline."""
    # Release metadata is advisory only when the version was explicit.
    info("Inspecting release metadata for the selected version and commit")
    release_issue, _, target_commit, warnings = inspect_release_context(context, version_text, target_branch)
    if not context.git.is_ancestor(target_commit, branch_tip):
        raise ReleaseWorkflowError(
            f"Release commit {target_commit} is not reachable from {context.config.remote}/{target_branch}"
        )

    if warnings:
        confirm(
            f"Continue preparing tag {version_text} despite these metadata warnings?",
            "Tag preparation declined; no changes were made",
            warnings=warnings,
            assume_yes=skip_confirmation,
            dry_run=context.dry_run,
        )
    else:
        info("Release issue, merged MR, and changelog entry were found")

    # The pipeline gate protects the exact commit that will be tagged.
    info("Checking the pipeline for the selected commit")
    pipeline = context.find_pipeline(target_commit, target_branch, pipeline_source="push")
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

    return release_issue, target_commit


def _select_tag_versions(
    context: ReleaseContext,
    release_version: CTAVersion,
    build_variants: tuple[BuildVariant, ...],
    variants_explicitly_selected: bool,
    release_candidate: bool,
) -> list[CTAVersion]:
    """Construct the ordered tags selected for one publication."""
    rc_number = None
    if release_candidate:
        rc_number = select_release_candidate(
            release_version,
            context.git.tags(),
        )
        info(f"Selected release candidate rc{rc_number}")

    selected_versions: list[CTAVersion] = []
    if not variants_explicitly_selected:
        selected_versions.append(release_version.with_components(release_candidate=rc_number))
    selected_versions.extend(
        release_version.with_components(release_candidate=rc_number, variant=build_variant)
        for build_variant in build_variants
    )

    return selected_versions


def build_tag_plan(
    context: ReleaseContext,
    version_text: str,
    target_branch: str,
    target_commit: str,
    release_issue: dict[str, Any] | None,
    selected_versions: list[CTAVersion],
    skip_confirmation: bool,
) -> TagPlan:
    """Finish all tag reads, editing, validation, and confirmation."""
    selected_tag_names = [version.text for version in selected_versions]

    # Display and validate the entire family before making any mutation.
    print("Tags selected for publication:")
    for tag_name in selected_tag_names:
        print(f"  {tag_name}")

    info("Checking every selected tag locally and remotely")
    _validate_selected_tags(context, selected_tag_names, target_commit)

    if context.dry_run:
        info("DRY-RUN: would open Git's editor for the tag description")
        tag_description = f"Release {version_text}"
    else:
        info("Opening Git's editor for the annotated tag description")
        tag_description = edit_tag_description(context, version_text, target_commit)

    descriptions = {tag.text: build_tag_description(tag_description, tag.variant) for tag in selected_versions}
    confirm(
        "Create and atomically push the selected tag family?",
        "Tag creation declined; no changes were made",
        assume_yes=skip_confirmation,
        dry_run=context.dry_run,
    )
    return TagPlan(version_text, target_branch, target_commit, release_issue, descriptions)


def execute_tag_plan(context: ReleaseContext, plan: TagPlan) -> list[str]:
    """Create and publish a fully preflighted tag family."""
    tag_names = list(plan.tag_descriptions)
    info(f"Creating {len(tag_names)} annotated local tag(s)")
    context.git.create_tags(plan.target_commit, plan.tag_descriptions)
    info(f"Pushing {len(tag_names)} tag(s) to GitLab")
    context.git.push_tags(context.config.remote, tag_names)
    return tag_names


def _report_tags_and_pipelines(
    context: ReleaseContext,
    release_issue: dict[str, Any] | None,
    version_text: str,
    target_commit: str,
    selected_tag_names: list[str],
) -> None:
    """Record tag progress and print every tag and pipeline link."""
    # Link the release issue and terminal output to every selected tag.
    commit_url = f"{context.config.project_web_url}/-/commit/{target_commit}"
    context.add_issue_note(
        release_issue,
        version_text,
        "tag",
        f"Tags {', '.join(f'`{tag_name}`' for tag_name in selected_tag_names)} created at commit "
        f"[`{target_commit[:12]}`]({commit_url}).",
    )
    for tag_name in selected_tag_names:
        tag_url = f"{context.config.project_web_url}/-/tags/{quote(tag_name, safe='')}"
        print(f"Tag page ({tag_name}): {tag_url}")

    # Tag pipelines are created asynchronously after the atomic push.
    tag_pipelines = _find_tag_pipelines(context, target_commit, selected_tag_names)
    pipeline_lines: list[str] = []
    for tag_name in selected_tag_names:
        tag_pipeline = tag_pipelines.get(tag_name)
        if tag_pipeline is None:
            print(f"Tag pipeline ({tag_name}) has not appeared yet; check GitLab shortly.")
            continue

        line = f"{tag_name}: {tag_pipeline['web_url']}"
        pipeline_lines.append(line)
        print(f"Tag pipeline ({tag_name}, {tag_pipeline['status']}): {tag_pipeline['web_url']}")

    if pipeline_lines:
        context.add_issue_note(
            release_issue,
            version_text,
            "pipeline",
            "Tag pipelines:\n" + "\n".join(f"- {line}" for line in pipeline_lines),
        )


def run(
    context: ReleaseContext,
    version_text: str,
    skip_confirmation: bool,
    target_branch: str = "main",
    release_candidate: bool = False,
    requested_suffixes: list[str] | None = None,
) -> None:
    """Resolve a revision, validate context, and publish a selected tag family."""
    target_branch = target_branch or context.config.default_branch
    info(f"Using explicitly requested release version {version_text}")

    release_version = CTAVersion.parse(version_text, require_base=True)
    remote_branch = f"{context.config.remote}/{target_branch}"
    info(f"Refreshing target branch {remote_branch!r}")
    branch_tip = context.git.resolve_remote_branch(
        context.config.remote,
        target_branch,
        fetch=not context.dry_run,
    )

    variants_explicitly_selected, build_variants = _select_build_variants(
        context,
        skip_confirmation,
        requested_suffixes,
    )

    release_issue, target_commit = _validate_release_metadata(
        context,
        version_text,
        branch_tip,
        skip_confirmation,
        target_branch,
    )
    print(f"Target branch tip: {remote_branch} at {branch_tip}")
    print(f"Merged changelog commit to tag for {version_text}: {target_commit}")

    selected_versions = _select_tag_versions(
        context,
        release_version,
        build_variants,
        variants_explicitly_selected,
        release_candidate,
    )

    plan = build_tag_plan(
        context,
        version_text,
        target_branch,
        target_commit,
        release_issue,
        selected_versions,
        skip_confirmation,
    )
    if context.dry_run:
        print(f"DRY-RUN: create and push tags: {', '.join(plan.tag_descriptions)}")
        return

    selected_tag_names = execute_tag_plan(context, plan)

    _report_tags_and_pipelines(
        context,
        plan.release_issue,
        version_text,
        target_commit,
        selected_tag_names,
    )
