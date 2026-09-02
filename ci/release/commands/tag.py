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
from typing import Any
from urllib.parse import quote

from commands import SubparserRegistry
from cta_version import (
    BUILD_VARIANTS,
    CTAVersion,
    BuildVariant,
    parse_build_variants,
    select_release_candidate,
)
from gitlab_api import GitLabAPIError
from release_context import ReleaseContext, ReleaseWorkflowError, info


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
    Existing RC families are inspected to choose or safely complete rcN.

Tag a specific commit, branch, or other Git revision:
  release tag v5.12.0.0-1 --ref origin/maintenance
  release tag v5.12.0.0-1 --ref 0123456789abcdef
    Without --ref, the latest origin/main commit is tagged.

Omit VERSION only after merging exactly one unfinished changelog MR:
  release tag
    Discovery fails rather than guessing when zero or multiple releases match.

Before publishing, the command prints the exact commit, checks release metadata,
requires a successful push pipeline for that commit, opens Git's editor for one
shared tag description, and validates all selected local and remote tags. Missing
tags are pushed together. Existing matching tags are safely reused; conflicting
tag targets stop the command.

Use "release --dry-run tag VERSION" to perform discovery and validation without
opening an editor, creating tags, pushing refs, or changing GitLab.""",
    )
    parser.add_argument("version", nargs="?")
    parser.add_argument(
        "--ref",
        dest="requested_target_ref",
        help="commit, branch, tag, or other Git revision to tag (default: latest origin/main)",
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
        help="automatically select and create the next or recoverable RC tag family",
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
        parsed_arguments.requested_target_ref,
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
    context: ReleaseContext, version_text: str, target_commit: str
) -> tuple[dict[str, Any] | None, list[str]]:
    """Return the release issue and non-fatal context validation warnings."""
    warnings: list[str] = []
    info(f"Looking for release ticket {context.config.issue_title(version_text)!r}")
    release_issue = context.find_or_create_release_issue(version_text, create=False)
    if release_issue is None:
        warnings.append(f"Release issue {context.config.issue_title(version_text)!r} was not found")

    info("Looking for the changelog merge request")
    changelog_merge_request = context.find_changelog_merge_request(version_text)
    if changelog_merge_request is None:
        warnings.append(f"Release merge request for {version_text} was not found")
    elif changelog_merge_request.get("state") != "merged":
        warnings.append(f"Release merge request {changelog_merge_request.get('web_url')} is not merged")

    try:
        info(f"Checking {context.config.changelog_file} at the selected commit for {version_text}")
        changelog, _ = context.read_repository_file(context.config.changelog_file, target_commit)
        heading_version = version_text.removeprefix("v")
        if not any(line.startswith("## ") and heading_version in line for line in changelog.splitlines()):
            warnings.append(f"{context.config.changelog_file} at {target_commit} has no heading for {version_text}")
    except GitLabAPIError as error:
        warnings.append(f"Could not inspect {context.config.changelog_file} at {target_commit}: {error}")

    return release_issue, warnings


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
    if skip_confirmation:
        return False, BUILD_VARIANTS
    if context.dry_run:
        info("DRY-RUN: would ask whether to include all PostgreSQL variants; selecting the base tag only")
        return False, build_variants

    answer = input("Also create pgsched, pgcat, and pgall tag variants? [y/N] ").strip().lower()
    return False, BUILD_VARIANTS if answer in ("y", "yes") else build_variants


def _validate_selected_tags(
    context: ReleaseContext,
    tag_names: list[str],
    target_commit: str,
) -> tuple[list[str], list[str]]:
    """Validate tag targets and return missing local and remote tags."""
    local_commits: dict[str, str | None] = {}
    remote_commits: dict[str, str | None] = {}

    for tag_name in tag_names:
        local_commits[tag_name] = context.git.local_tag_commit(tag_name)
        remote_commits[tag_name] = context.git.remote_tag_commit(context.config.remote, tag_name)
        local_commit = local_commits[tag_name]
        remote_commit = remote_commits[tag_name]

        if (local_commit and local_commit != target_commit) or (remote_commit and remote_commit != target_commit):
            raise ReleaseWorkflowError(
                f"HIGH SEVERITY: {tag_name} does not point to expected commit {target_commit} "
                f"(local={local_commit}, remote={remote_commit})"
            )

    return (
        [tag_name for tag_name in tag_names if local_commits[tag_name] is None],
        [tag_name for tag_name in tag_names if remote_commits[tag_name] is None],
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
    target_commit: str,
    version_was_explicit: bool,
    skip_confirmation: bool,
) -> tuple[dict[str, Any] | None, bool]:
    """Validate release metadata and the exact commit pipeline."""
    # Release metadata is advisory only when the version was explicit.
    info("Inspecting release metadata for the selected version and commit")
    release_issue, warnings = inspect_release_context(context, version_text, target_commit)
    confirmation_received = skip_confirmation

    if warnings:
        for warning in warnings:
            print(f"WARNING: {warning}", file=sys.stderr)
        if not version_was_explicit:
            raise ReleaseWorkflowError(
                "Inferred release context is incomplete; rerun release tag with an explicit version to override"
            )
        if not skip_confirmation and not context.dry_run:
            answer = input(f"Continue and create tag {version_text} despite these warnings? [y/N] ").strip().lower()
            if answer not in ("y", "yes"):
                raise ReleaseWorkflowError("Tag creation declined; no changes were made")
            confirmation_received = True
    else:
        info("Release issue, merged MR, and changelog entry were found")

    # The pipeline gate protects the exact commit that will be tagged.
    info("Checking the pipeline for the selected commit")
    pipeline = context.find_pipeline(target_commit, pipeline_source="push")
    pipeline_status = "not found" if pipeline is None else str(pipeline.get("status") or "unknown")
    if context.config.require_successful_target_pipeline and pipeline_status != "success":
        pipeline_url = pipeline.get("web_url") if pipeline else None
        warning = f"Pipeline for release commit {target_commit} is {pipeline_status}"
        if pipeline_url:
            warning += f": {pipeline_url}"
        print(f"WARNING: {warning}", file=sys.stderr)
        if not skip_confirmation and not context.dry_run:
            answer = (
                input(f"Continue and create tag {version_text} without a successful pipeline? [y/N] ").strip().lower()
            )
            if answer not in ("y", "yes"):
                raise ReleaseWorkflowError("Tag creation declined; no changes were made")
            confirmation_received = True
    elif pipeline is None:
        info("No pipeline found; the pipeline gate is disabled")
    else:
        info(f"Found successful pipeline: {pipeline.get('web_url') or pipeline.get('id', 'unknown')}")

    return release_issue, confirmation_received


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
            build_variants,
            variants_explicitly_selected=variants_explicitly_selected,
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


def _publish_selected_tags(
    context: ReleaseContext,
    version_text: str,
    target_commit: str,
    selected_versions: list[CTAVersion],
    confirmation_received: bool,
) -> list[str]:
    """Create missing annotated tags and atomically publish missing refs."""
    selected_tag_names = [version.text for version in selected_versions]

    # Display and validate the entire family before making any mutation.
    print("Tags selected for publication:")
    for tag_name in selected_tag_names:
        print(f"  {tag_name}")

    info("Checking every selected tag locally and remotely")
    missing_local_tags, missing_remote_tags = _validate_selected_tags(
        context,
        selected_tag_names,
        target_commit,
    )
    if not missing_remote_tags:
        info("All selected tags already exist remotely at the expected commit")
        return selected_tag_names

    if not confirmation_received and not context.dry_run:
        answer = input("Create and push the selected tags? [y/N] ").strip().lower()
        if answer not in ("y", "yes"):
            raise ReleaseWorkflowError("Tag creation declined; no changes were made")

    # One editor description is shared by all missing local tags.
    if missing_local_tags and context.dry_run:
        info("DRY-RUN: would open Git's editor for the tag description")
        tag_description = f"Release {version_text}"
    elif missing_local_tags:
        info("Opening Git's editor for the annotated tag description")
        tag_description = edit_tag_description(context, version_text, target_commit)
    else:
        tag_description = ""

    descriptions = {
        tag.text: build_tag_description(tag_description, tag.variant)
        for tag in selected_versions
        if tag.text in missing_local_tags
    }
    if descriptions:
        info(f"Creating {len(descriptions)} annotated local tag(s)")
        context.git.create_tags(target_commit, descriptions)

    # Publish every missing tag together.
    info(f"Pushing {len(missing_remote_tags)} tag(s) to GitLab")
    context.git.push_tags(context.config.remote, missing_remote_tags)

    return selected_tag_names


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
    version_text: str | None,
    skip_confirmation: bool,
    requested_target_ref: str | None,
    release_candidate: bool = False,
    requested_suffixes: list[str] | None = None,
) -> None:
    """Resolve a revision, validate context, and publish a selected tag family."""
    # Resolve the base release and requested build variants.
    version_was_explicit = version_text is not None
    if version_text is None:
        info("No version provided; attempting release version discovery")
        version_text = context.discover_unfinished_release_version()
    else:
        info(f"Using explicitly requested release version {version_text}")

    release_version = CTAVersion.parse(version_text, require_base=True)
    variants_explicitly_selected, build_variants = _select_build_variants(
        context,
        skip_confirmation,
        requested_suffixes,
    )

    # Resolve the exact commit independently of the local checkout branch.
    target_ref = requested_target_ref or f"{context.config.remote}/{context.config.default_branch}"
    info(f"Resolving tag target {target_ref!r}")

    target_commit = context.git.resolve_tag_target(
        context.config.remote,
        context.config.default_branch,
        target_ref,
        fetch=not context.dry_run,
    )
    print(f"Tag target: {target_ref}")
    print(f"Commit to tag for {version_text}: {target_commit}")

    # Validate metadata and construct the complete tag family before mutation.
    release_issue, confirmation_received = _validate_release_metadata(
        context,
        version_text,
        target_commit,
        version_was_explicit,
        skip_confirmation,
    )

    selected_versions = _select_tag_versions(
        context,
        release_version,
        build_variants,
        variants_explicitly_selected,
        release_candidate,
    )

    # Publish and report the selected tags.
    selected_tag_names = _publish_selected_tags(
        context,
        version_text,
        target_commit,
        selected_versions,
        confirmation_received,
    )

    _report_tags_and_pipelines(
        context,
        release_issue,
        version_text,
        target_commit,
        selected_tag_names,
    )
