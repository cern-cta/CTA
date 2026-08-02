# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Developer-facing CTA release workflow."""

from __future__ import annotations

import argparse
import base64
import getpass
import os
import shlex
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from cta_version import CTAVersion, VersionError, previous_release
from git_repo import Git, GitError, discover_repository_root
from gitlab_api import GitLabAPI, GitLabAPIError
from release_config import ReleaseConfig

TOKEN_FILE = Path.home() / ".config" / "cta" / "gitlab-api-token"
NOTE_MARKER = "<!-- cta-release:{version}:{stage} -->"
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


class ReleaseWorkflowError(RuntimeError):
    """A failure while coordinating an otherwise valid release workflow."""


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


def info(message: str) -> None:
    """Print one high-level release workflow step."""
    print(f"==> {message}")


def load_token(dry_run: bool) -> str:
    """Load a GitLab token from the environment, shared file, or prompt."""
    token = os.environ.get("GITLAB_TOKEN")
    if token:
        return token
    if TOKEN_FILE.is_file():
        return TOKEN_FILE.read_text(encoding="utf-8").strip()
    if dry_run:
        raise ReleaseWorkflowError("Set GITLAB_TOKEN or create ~/.config/cta-ci-debug/token to run diagnostics")
    if not sys.stdin.isatty():
        raise ReleaseWorkflowError("GITLAB_TOKEN is required in a non-interactive terminal")
    print("GitLab authentication is required (token needs the api scope).")
    token = getpass.getpass("Token: ").strip()
    if not token:
        raise ReleaseWorkflowError("No GitLab token was provided")
    if input(f"Store token in {TOKEN_FILE}? [Y/n] ").strip().lower() in (
        "",
        "y",
        "yes",
    ):
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(token, encoding="utf-8")
        TOKEN_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return token


class ReleaseService:
    """Coordinate idempotent CTA release operations across Git and GitLab."""

    def __init__(
        self,
        root: Path,
        config: ReleaseConfig,
        api: GitLabAPI,
        dry_run: bool,
        allow_unclean: bool = False,
    ):
        """Create a release workflow service for one repository."""
        self.root = root
        self.config = config
        self.api = api
        self.dry_run = dry_run
        self.git = Git(root, dry_run=dry_run, allow_unclean=allow_unclean)

    def find_release_issues(self, version: str) -> list[dict[str, Any]]:
        """Find release-labelled issues with the deterministic version title."""
        title = self.config.issue_title(version)
        return [
            issue
            for issue in self.api.get_all(
                "issues",
                {
                    "search": title,
                    "in": "title",
                    "labels": self.config.release_label,
                    "scope": "all",
                },
            )
            if issue.get("title") == title
        ]

    def find_or_create_release_issue(self, version: str, create: bool) -> dict[str, Any] | None:
        """Find the unique release issue, optionally creating it when absent."""
        issues = self.find_release_issues(version)
        if len(issues) > 1:
            raise ReleaseWorkflowError(
                f"Multiple issues titled {self.config.issue_title(version)!r} exist; resolve duplicates"
            )
        if issues:
            info(f"Reusing release issue {issues[0]['web_url']}")
            return issues[0]
        if not create:
            return None
        if self.dry_run:
            print(f"DRY-RUN: create issue {self.config.issue_title(version)!r}")
            return None
        info(f"Creating release issue for {version}")
        template = (self.root / self.config.issue_template).read_text(encoding="utf-8")
        return self.api.post(
            "issues",
            json={
                "title": self.config.issue_title(version),
                "description": template,
                "labels": self.config.release_label,
            },
        )

    def add_issue_note(self, issue: dict[str, Any] | None, version: str, stage: str, body: str) -> None:
        """Add one idempotent, stage-marked progress note to a release issue."""
        marker = NOTE_MARKER.format(version=version, stage=stage)
        if issue is None:
            if self.dry_run:
                print(f"DRY-RUN: add release issue note: {body}")
            return
        iid = issue["iid"]
        notes = self.api.get_all(f"issues/{iid}/notes")
        if any(marker in note.get("body", "") for note in notes):
            return
        if self.dry_run:
            print(f"DRY-RUN: add release issue note: {body}")
            return
        self.api.post(f"issues/{iid}/notes", json={"body": f"{marker}\n{body}"})

    def find_release_merge_requests(self, version: str | None = None) -> list[dict[str, Any]]:
        """Find release merge requests, optionally restricted to one version."""
        params: dict[str, Any] = {
            "scope": "all",
            "target_branch": self.config.default_branch,
        }
        if version:
            params["source_branch"] = self.config.changelog_branch(version)
        return self.api.get_all("merge_requests", params)

    def find_release_merge_request(self, version: str) -> dict[str, Any] | None:
        """Find the unique deterministic merge request for a release."""
        matches = [
            mr
            for mr in self.find_release_merge_requests(version)
            if mr.get("source_branch") == self.config.changelog_branch(version)
            and mr.get("title") == self.config.merge_request_title(version)
        ]
        if len(matches) > 1:
            raise ReleaseWorkflowError(f"Multiple release MRs exist for {version}; resolve duplicates")
        return matches[0] if matches else None

    def discover_unfinished_release_version(self) -> str:
        """Infer the sole merged release whose tag has not been pushed."""
        info("Searching GitLab for merged release MRs without a corresponding tag")
        candidates: list[str] = []
        for mr in self.find_release_merge_requests():
            source = str(mr.get("source_branch", ""))
            if mr.get("state") != "merged" or not source.endswith(self.config.branch_suffix):
                continue
            version = source[: -len(self.config.branch_suffix)]
            try:
                CTAVersion.parse(version)
            except VersionError:
                continue
            if self.git.remote_tag_commit(self.config.remote, version) is None:
                candidates.append(version)
        candidates = sorted(set(candidates), key=lambda item: CTAVersion.parse(item).core)
        info(f"Found {len(candidates)} unfinished release candidate(s)")
        if not candidates:
            raise ReleaseWorkflowError("No merged, unfinished release merge request was found")
        if len(candidates) > 1:
            listing = "\n".join(f"  {candidate}" for candidate in candidates)
            raise ReleaseWorkflowError(
                f"Multiple unfinished releases were found:\n\n{listing}\n\nRun:\nrelease tag {candidates[-1]}"
            )
        print(f"Inferred release version: {candidates[0]}")
        return candidates[0]

    def _validate_tag_is_available(self, version: CTAVersion) -> None:
        """Ensure the exact release tag does not already exist."""
        if self.git.local_tag_commit(version.text) or self.git.remote_tag_commit(self.config.remote, version.text):
            raise ReleaseWorkflowError(f"Tag {version.text} already exists; use release status {version.text}")

    def _edit_notes(self, notes: str, version: str) -> str:
        """Open generated notes in Git's selected editor and validate them."""
        editor = self.git.editor_command()
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".md", encoding="utf-8") as candidate:
            candidate.write(notes)
            candidate.flush()
            command = [*shlex.split(editor), candidate.name]
            result = subprocess.run(command, check=False)
            if result.returncode:
                raise ReleaseWorkflowError(f"Editor exited with status {result.returncode}")
            candidate.seek(0)
            edited = candidate.read().strip()
        heading_version = version.removeprefix("v")
        if not edited or not any(line.startswith("## ") and heading_version in line for line in edited.splitlines()):
            raise ReleaseWorkflowError(f"Edited changelog must contain a level-two heading for {heading_version}")
        return edited + "\n"

    def edit_tag_description(self, version: str, commit: str) -> str:
        """Open Git's editor and require a non-empty annotated tag description."""
        editor = self.git.editor_command()
        template = (
            "\n"
            f"# Enter a short description for tag {version}.\n"
            f"# The tag will point to {commit}.\n"
            "# Lines starting with '#' are ignored.\n"
        )
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".txt", encoding="utf-8") as description_file:
            description_file.write(template)
            description_file.flush()
            result = subprocess.run([*shlex.split(editor), description_file.name], check=False)
            if result.returncode:
                raise ReleaseWorkflowError(f"Editor exited with status {result.returncode}")
            description_file.seek(0)
            description = "\n".join(
                line for line in description_file.read().splitlines() if not line.lstrip().startswith("#")
            ).strip()
        if not description:
            raise ReleaseWorkflowError("Tag description is empty; the tag was not created")
        return description

    def generate_changelog_notes(self, from_commit: str, to_commit: str, version: str) -> str:
        """Generate notes and surface commits that lack valid changelog trailers."""
        response = self.api.get(
            "repository/changelog",
            {"from": from_commit, "to": to_commit, "version": version},
        )
        if not isinstance(response, dict) or not isinstance(response.get("notes"), str):
            raise ReleaseWorkflowError("GitLab returned an invalid repository/changelog response")
        commits = self.api.get_all(
            "repository/commits",
            {
                "ref_name": f"{from_commit}..{to_commit}",
                "trailers": True,
            },
        )
        missing_trailers = [
            commit for commit in commits if commit.get("trailers", {}).get("Changelog") not in CHANGELOG_CATEGORIES
        ]
        notes = response["notes"].rstrip()
        if missing_trailers:
            entries = "\n".join(f"- [{commit['title']}]({commit['web_url']})" for commit in missing_trailers)
            notes += (
                "\n\n### Commits missing valid Changelog trailers\n\n"
                "<!-- Remove these entries or move them to the correct category before saving. -->\n\n"
                f"{entries}"
            )
        return notes + "\n"

    def read_repository_file(self, path: str, ref: str) -> tuple[str, str]:
        """Read and decode a repository file from GitLab at a specific ref."""
        result = self.api.get(f"repository/files/{quote(path, safe='')}", {"ref": ref})
        content = base64.b64decode(result["content"]).decode()
        return content, result["blob_id"]

    def changelog_issue_note(
        self,
        version: str,
        commit: str,
        branch: str,
        branch_url: str,
        merge_request: dict[str, Any],
    ) -> str:
        """Build the single issue comment summarizing changelog preparation."""
        commit_url = f"{self.config.project_web_url}/-/commit/{commit}"
        return "\n".join(
            [
                f"The commit produced by merging "
                f"[merge request !{merge_request['iid']}]({merge_request['web_url']}) "
                f"will be tagged as **{version}**.",
                "",
                f"The changelog covers changes through commit "
                f"[`{commit[:12]}`]({commit_url}) and was generated on branch "
                f"[`{branch}`]({branch_url}).",
                "",
                "Awaiting MR approval and merge.",
            ]
        )

    def prepare_release(self, version_text: str) -> None:
        """Create or reuse the issue, changelog branch, and merge request."""
        version = CTAVersion.parse(version_text)
        info("Validating the local repository")
        commit = self.git.validate_repository(self.config.default_branch, self.config.remote, fetch=not self.dry_run)
        info("Checking GitLab authentication")
        user_id = authenticated_user_id(self.api.authenticate())
        info(f"Checking release state for {version.text}")
        existing_mr = self.find_release_merge_request(version.text)
        if existing_mr and existing_mr.get("state") == "merged":
            print(f"{version.text} is already prepared and merged. Run: release tag {version.text}")
            return
        self._validate_tag_is_available(version)
        issue = self.find_or_create_release_issue(version.text, create=True)
        if issue is not None:
            info(f"Release ticket: {issue['web_url']}")
        previous = previous_release(version, self.git.tags())
        info(f"Generating changelog from {previous.text} through {commit}")
        notes = self.generate_changelog_notes(
            previous.text,
            commit,
            version.text.removeprefix("v"),
        )
        if self.dry_run:
            print(f"DRY-RUN: open generated changelog for {version.text} in Git's editor")
            print(f"DRY-RUN: create or verify branch {self.config.changelog_branch(version.text)} at {commit}")
            print(f"DRY-RUN: update {self.config.changelog_file} and create or reuse its merge request")
            return
        if issue is None:
            raise ReleaseWorkflowError("Release issue creation returned no issue; cannot create a linked merge request")
        info("Opening the generated changelog in Git's editor")
        edited = self._edit_notes(notes, version.text)
        branch = self.config.changelog_branch(version.text)
        info(f"Creating or verifying changelog branch {branch}")
        branches = self.api.get_all("repository/branches", {"search": f"^{escape_branch_search(branch)}$"})
        exact_branches = [item for item in branches if item.get("name") == branch]
        if exact_branches:
            branch_resource = exact_branches[0]
            if exact_branches[0]["commit"]["id"] != commit and existing_mr is None:
                raise ReleaseWorkflowError(f"Existing branch {branch} does not start at expected commit {commit}")
        else:
            branch_resource = self.api.post("repository/branches", params={"branch": branch, "ref": commit})
        info(f"Updating {self.config.changelog_file} on {branch}")
        old_changelog, _ = self.read_repository_file(self.config.changelog_file, branch)
        heading_version = version.text.removeprefix("v")
        has_release_heading = any(
            line.startswith("## ") and heading_version in line for line in old_changelog.splitlines()
        )
        if has_release_heading:
            print(f"Reusing existing changelog entry for {version.text} on {branch}.")
        elif existing_mr is not None:
            raise ReleaseWorkflowError(
                f"Existing release MR {existing_mr['web_url']} does not contain the expected "
                f"{version.text} changelog heading; inspect it before rerunning prepare"
            )
        else:
            self.api.put(
                f"repository/files/{quote(self.config.changelog_file, safe='')}",
                json={
                    "branch": branch,
                    "content": edited + "\n" + old_changelog.lstrip(),
                    "commit_message": f"[Misc] Update changelog for release {version.text.removeprefix('v')}",
                },
            )
        info("Creating or reusing the changelog merge request")
        mr = self.find_release_merge_request(version.text)
        if mr is None:
            mr = self.api.post(
                "merge_requests",
                json={
                    "source_branch": branch,
                    "target_branch": self.config.default_branch,
                    "title": self.config.merge_request_title(version.text),
                    "description": merge_request_description(version.text, issue["iid"]),
                    "labels": ",".join(MERGE_REQUEST_LABELS),
                    "assignee_ids": [user_id],
                    "reviewer_ids": [user_id],
                    "remove_source_branch": True,
                    "squash": True,
                },
            )
        branch_url = branch_resource.get(
            "web_url",
            f"{self.config.project_web_url}/-/tree/{quote(branch, safe='')}",
        )
        self.add_issue_note(
            issue,
            version.text,
            "changelog",
            self.changelog_issue_note(
                version.text,
                commit,
                branch,
                branch_url,
                mr,
            ),
        )
        issue_summary = f"\n\nRelease ticket:\n{issue['web_url']}"
        print(
            f"\nRelease {version.text} prepared."
            f"{issue_summary}"
            f"\n\nReview and merge:\n{mr['web_url']}"
            "\n\nAfter it is merged, run:\nrelease tag"
        )

    def load_release_context(
        self, version_text: str, validate_local: bool
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        """Validate and reconstruct a release issue, MR, and target commit."""
        info(f"Reconstructing release context for {version_text}")
        CTAVersion.parse(version_text)
        if validate_local:
            self.git.validate_repository(self.config.default_branch, self.config.remote, fetch=not self.dry_run)
        issue = self.find_or_create_release_issue(version_text, create=False)
        if issue is None:
            raise ReleaseWorkflowError(f"Release issue {self.config.issue_title(version_text)!r} does not exist")
        mr = self.find_release_merge_request(version_text)
        if mr is None:
            raise ReleaseWorkflowError(f"Release MR for {version_text} does not exist")
        if mr.get("state") != "merged" or mr.get("target_branch") != self.config.default_branch:
            raise ReleaseWorkflowError(
                f"Release MR {mr.get('web_url')} has not been merged into {self.config.default_branch}"
            )
        commit = mr.get("squash_commit_sha") or mr.get("merge_commit_sha")
        if not commit:
            raise ReleaseWorkflowError(f"Release MR {mr.get('web_url')} has no resulting commit")
        refs = self.api.get_all(f"repository/commits/{commit}/refs", {"type": "branch"})
        if not any(ref.get("name") == self.config.default_branch for ref in refs):
            raise ReleaseWorkflowError(f"Release commit {commit} is not on {self.config.default_branch}")
        changelog, _ = self.read_repository_file(self.config.changelog_file, commit)
        if version_text.removeprefix("v") not in changelog:
            raise ReleaseWorkflowError(f"{self.config.changelog_file} at {commit} does not contain {version_text}")
        return issue, mr, commit

    def find_pipeline(
        self,
        commit: str,
        ref: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any] | None:
        """Find the newest pipeline for an exact commit and optional ref."""
        params: dict[str, Any] = {"sha": commit}
        if ref:
            params["ref"] = ref
        if source:
            params["source"] = source
        pipelines = self.api.get_all("pipelines", params)
        return pipelines[0] if pipelines else None

    def show_release_status(self, version_text: str | None) -> None:
        """Print reconstructed Git and GitLab state for a release."""
        version_text = version_text or self.discover_unfinished_release_version()
        info(f"Inspecting release status for {version_text}")
        issue, mr, commit = self.load_release_context(version_text, validate_local=False)
        pipeline = self.find_pipeline(commit, self.config.default_branch, source="push")
        local_tag = self.git.local_tag_commit(version_text)
        remote_tag = self.git.remote_tag_commit(self.config.remote, version_text)
        print(f"Version:  {version_text}")
        print(f"Issue:    {issue['web_url']}")
        print(f"MR:       {mr['web_url']} ({mr['state']})")
        print(f"Commit:   {commit}")
        print(f"Pipeline: {pipeline['status'] if pipeline else 'not found'}")
        print(f"Tag:      local={local_tag or 'absent'}, remote={remote_tag or 'absent'}")

    def inspect_tag_release_context(self, version_text: str, commit: str) -> tuple[dict[str, Any] | None, list[str]]:
        """Return the release issue and non-fatal context validation warnings."""
        warnings: list[str] = []
        info(f"Looking for release ticket {self.config.issue_title(version_text)!r}")
        issue = self.find_or_create_release_issue(version_text, create=False)
        if issue is None:
            warnings.append(f"Release issue {self.config.issue_title(version_text)!r} was not found")
        info("Looking for the changelog merge request")
        merge_request = self.find_release_merge_request(version_text)
        if merge_request is None:
            warnings.append(f"Release merge request for {version_text} was not found")
        elif merge_request.get("state") != "merged":
            warnings.append(f"Release merge request {merge_request.get('web_url')} is not merged")
        try:
            info(f"Checking {self.config.changelog_file} at the selected commit for {version_text}")
            changelog, _ = self.read_repository_file(self.config.changelog_file, commit)
            heading_version = version_text.removeprefix("v")
            if not any(line.startswith("## ") and heading_version in line for line in changelog.splitlines()):
                warnings.append(f"{self.config.changelog_file} at {commit} has no heading for {version_text}")
        except GitLabAPIError as error:
            warnings.append(f"Could not inspect {self.config.changelog_file} at {commit}: {error}")
        return issue, warnings

    def tag_release(self, version_text: str | None, yes: bool, target_ref: str | None) -> None:
        """Resolve a revision, validate release context, and create its tag."""
        version_was_explicit = version_text is not None
        if version_text is None:
            info("No version provided; attempting release version discovery")
            version_text = self.discover_unfinished_release_version()
        else:
            info(f"Using explicitly requested release version {version_text}")
        CTAVersion.parse(version_text)
        if target_ref is None:
            info(f"No tag target provided; selecting the latest {self.config.remote}/{self.config.default_branch}")
        else:
            info(f"Resolving requested tag target {target_ref!r}")
        resolved_ref, commit = self.git.resolve_tag_target(
            self.config.remote,
            self.config.default_branch,
            ref=target_ref,
            fetch=not self.dry_run,
        )
        print(f"Tag target: {resolved_ref}")
        print(f"Commit to tag for {version_text}: {commit}")
        info("Inspecting release metadata for the selected version and commit")
        issue, warnings = self.inspect_tag_release_context(version_text, commit)
        confirmed = yes
        if warnings:
            for warning in warnings:
                print(f"WARNING: {warning}", file=sys.stderr)
            if not version_was_explicit:
                raise ReleaseWorkflowError(
                    "Inferred release context is incomplete; rerun release tag with an explicit version to override"
                )
            if not yes and not self.dry_run:
                answer = input(f"Continue and create tag {version_text} despite these warnings? [y/N] ").strip().lower()
                if answer not in ("y", "yes"):
                    raise ReleaseWorkflowError("Tag creation declined; no changes were made")
                confirmed = True
        else:
            info("Release issue, merged MR, and changelog entry were found")
        info("Checking the pipeline for the selected commit")
        pipeline_ref = self.config.default_branch if target_ref is None else None
        pipeline_source = "push" if target_ref is None else None
        pipeline = self.find_pipeline(commit, pipeline_ref, pipeline_source)
        if pipeline is None:
            if self.config.require_pipeline:
                raise ReleaseWorkflowError(
                    f"Pipeline for release commit {commit} was not found; wait for a successful pipeline"
                )
            info("No pipeline found; the pipeline gate is disabled")
        else:
            if self.config.require_pipeline and pipeline.get("status") != "success":
                raise ReleaseWorkflowError(
                    f"Pipeline for release commit {commit} is {pipeline.get('status')}; wait for a successful pipeline"
                )
            pipeline_reference = pipeline.get("web_url") or pipeline.get("id", "unknown")
            info(f"Found successful pipeline: {pipeline_reference}")
        local_tag = self.git.local_tag_commit(version_text)
        remote_tag = self.git.remote_tag_commit(self.config.remote, version_text)
        info(f"Checking whether tag {version_text} already exists locally or remotely")
        if local_tag or remote_tag:
            if (local_tag and local_tag != commit) or (remote_tag and remote_tag != commit):
                raise ReleaseWorkflowError(
                    f"HIGH SEVERITY: {version_text} does not point to expected commit {commit} "
                    f"(local={local_tag}, remote={remote_tag})"
                )
            if remote_tag == commit:
                info("The remote tag already points to the selected commit")
                print(f"Release tag {version_text} already points to {commit}.")
            elif local_tag == commit:
                info("Pushing the existing local tag to GitLab")
                self.git.run(
                    [
                        "push",
                        self.config.remote,
                        f"refs/tags/{version_text}:refs/tags/{version_text}",
                    ],
                    mutate=True,
                )
        else:
            if not confirmed and not self.dry_run:
                answer = input(f"Create and push annotated tag {version_text}? [y/N] ").strip().lower()
                if answer not in ("y", "yes"):
                    raise ReleaseWorkflowError("Tag creation declined; no changes were made")
            if self.dry_run:
                info("DRY-RUN: would open Git's editor for the tag description")
                tag_description = f"Release {version_text}"
            else:
                info("Opening Git's editor for the annotated tag description")
                tag_description = self.edit_tag_description(version_text, commit)
            info(f"Creating annotated tag {version_text} and pushing it to GitLab")
            self.git.create_tag(self.config.remote, version_text, commit, tag_description)
        commit_url = f"{self.config.project_web_url}/-/commit/{commit}"
        self.add_issue_note(
            issue,
            version_text,
            "tag",
            f"Tag `{version_text}` created at commit [`{commit[:12]}`]({commit_url}).",
        )
        tag_pipeline = None
        tag_url = f"{self.config.project_web_url}/-/tags/{quote(version_text, safe='')}"
        print(f"Tag page: {tag_url}")
        if not self.dry_run:
            info("Looking for the tag pipeline")
            for _ in range(6):
                tag_pipeline = self.find_pipeline(commit, version_text, source="push")
                if tag_pipeline:
                    break
                time.sleep(2)
        if tag_pipeline:
            self.add_issue_note(
                issue,
                version_text,
                "pipeline",
                f"Tag pipeline: {tag_pipeline['web_url']}",
            )
            print(f"Tag pipeline ({tag_pipeline['status']}): {tag_pipeline['web_url']}")
        else:
            print("Tag pipeline has not appeared yet; check GitLab pipelines shortly.")


def escape_branch_search(value: str) -> str:
    """Escape CTA branch punctuation for GitLab's RE2 branch search."""
    return value.replace(".", r"\.")


def create_argument_parser() -> argparse.ArgumentParser:
    """Build the developer-facing release command parser."""
    result = argparse.ArgumentParser(
        prog="release",
        description="Prepare and tag CTA releases through GitLab.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  release prepare v5.12.0.0-1
  release tag
  release tag v5.12.0.0-1 --ref origin/main
  release --dry-run tag v5.12.0.0-1
  release status v5.12.0.0-1""",
    )
    result.add_argument(
        "--dry-run",
        action="store_true",
        help="print planned mutations without making them",
    )
    result.add_argument(
        "--allow-unclean",
        action="store_true",
        help="allow a dirty or non-main checkout while still resolving the tag target explicitly",
    )
    commands = result.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare", help="create the release issue and changelog MR")
    prepare.add_argument("version")
    tag = commands.add_parser("tag", help="validate release context and create an annotated tag")
    tag.add_argument("version", nargs="?")
    tag.add_argument(
        "--ref",
        dest="target_ref",
        help="commit, branch, tag, or other Git revision to tag (default: latest origin/main)",
    )
    tag.add_argument("--yes", action="store_true", help="create the tag without confirmation")
    status = commands.add_parser("status", help="show reconstructed release state")
    status.add_argument("version", nargs="?")
    return result


def main(argv: list[str] | None = None) -> int:
    """Run the requested release command and convert failures to exit codes."""
    args = create_argument_parser().parse_args(argv)
    try:
        root = discover_repository_root()
        config = ReleaseConfig()
        api = GitLabAPI(config.gitlab_url, config.project_id, load_token(args.dry_run))
        service = ReleaseService(root, config, api, args.dry_run, allow_unclean=args.allow_unclean)
        if args.command == "prepare":
            service.prepare_release(args.version)
        elif args.command == "tag":
            service.tag_release(args.version, args.yes, args.target_ref)
        else:
            service.show_release_status(args.version)
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
    else:
        return 0


if __name__ == "__main__":
    sys.exit(main())
