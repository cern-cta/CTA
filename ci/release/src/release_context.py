# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Shared Git and GitLab state used by release commands."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any
from urllib.parse import quote

from cta_version import CTAVersion
from errors import ReleaseWorkflowError
from git_repo import Git
from gitlab_api import GitLabAPI
from release_config import ReleaseConfig


def info(message: str) -> None:
    """Print one high-level release workflow step."""
    print(f"==> {message}")


class ReleaseContext:
    """Provide shared repository and GitLab operations to release commands."""

    def __init__(
        self,
        root: Path,
        config: ReleaseConfig,
        api: GitLabAPI,
        dry_run: bool,
    ):
        """Create a release context for one repository and invocation."""
        self.root = root
        self.config = config
        self.api = api
        self.dry_run = dry_run
        self.git = Git(root, dry_run=dry_run)

    def find_release_issues(self, version: str) -> list[dict[str, Any]]:
        """Find release-labelled issues with the deterministic version title."""
        issue_title = self.config.issue_title(version)

        return [
            release_issue
            for release_issue in self.api.get_all(
                "issues",
                {
                    "search": issue_title,
                    "in": "title",
                    "labels": self.config.release_label,
                    "scope": "all",
                },
            )
            if release_issue.get("title") == issue_title
        ]

    def find_or_create_release_issue(self, version: str, create: bool) -> dict[str, Any] | None:
        """Find the unique release issue, optionally creating it when absent."""
        release_issues = self.find_release_issues(version)

        if len(release_issues) > 1:
            raise ReleaseWorkflowError(
                f"Multiple issues titled {self.config.issue_title(version)!r} exist; resolve duplicates"
            )
        if release_issues:
            info(f"Reusing release issue {release_issues[0]['web_url']}")
            return release_issues[0]
        if not create:
            return None

        if self.dry_run:
            print(f"DRY-RUN: create issue {self.config.issue_title(version)!r}")
            return None

        return self.create_release_issue(version)

    def create_release_issue(self, version: str) -> dict[str, Any]:
        """Create a release issue without repeating preflight discovery."""
        info(f"Creating release issue for {version}")
        issue_description = (self.root / self.config.issue_template).read_text(encoding="utf-8")

        return self.api.post(
            "issues",
            json={
                "title": self.config.issue_title(version),
                "description": issue_description,
                "labels": self.config.release_label,
            },
        )

    def add_issue_note(
        self,
        release_issue: dict[str, Any] | None,
        version: str,
        stage: str,
        body: str,
    ) -> None:
        """Add an informational progress note to a release issue."""
        del version, stage
        if release_issue is None:
            if self.dry_run:
                print(f"DRY-RUN: add release issue note: {body}")
            return

        if self.dry_run:
            print(f"DRY-RUN: add release issue note: {body}")
            return

        self.api.post(f"issues/{release_issue['iid']}/notes", json={"body": body})

    def find_changelog_merge_requests(
        self,
        version: str,
        target_branch: str,
    ) -> list[dict[str, Any]]:
        """Find active changelog MRs and reject source-branch collisions."""
        source_branch = self.config.changelog_branch(version, target_branch)
        active_merge_requests = [
            merge_request
            for merge_request in self.api.get_all(
                "merge_requests",
                {"scope": "all", "source_branch": source_branch},
            )
            if merge_request.get("source_branch") == source_branch
            and merge_request.get("state") in ("opened", "merged")
        ]
        if any(item.get("target_branch") != target_branch for item in active_merge_requests):
            raise ReleaseWorkflowError(
                f"Changelog branch {source_branch} is already used by an active MR targeting another branch"
            )
        return [
            item
            for item in active_merge_requests
            if item.get("title") == self.config.changelog_merge_request_title(version)
        ]

    def find_changelog_merge_request(
        self,
        version: str,
        target_branch: str,
    ) -> dict[str, Any] | None:
        """Find the unique deterministic changelog merge request for a release."""
        matching_merge_requests = list(self.find_changelog_merge_requests(version, target_branch))

        if len(matching_merge_requests) > 1:
            raise ReleaseWorkflowError(f"Multiple changelog MRs exist for {version}; resolve duplicates")

        return matching_merge_requests[0] if matching_merge_requests else None

    def read_repository_file(self, path: str, ref: str) -> tuple[str, str]:
        """Read and decode a repository file from GitLab at a specific ref."""
        file_metadata = self.api.get(f"repository/files/{quote(path, safe='')}", {"ref": ref})
        file_content = base64.b64decode(file_metadata["content"]).decode()

        return file_content, file_metadata["blob_id"]

    def find_pipeline(
        self,
        target_commit: str,
        pipeline_ref: str | None = None,
        pipeline_source: str | None = None,
    ) -> dict[str, Any] | None:
        """Find the newest pipeline for an exact commit and optional ref."""
        query_parameters: dict[str, Any] = {"sha": target_commit}
        if pipeline_ref:
            query_parameters["ref"] = pipeline_ref
        if pipeline_source:
            query_parameters["source"] = pipeline_source

        pipelines = self.api.get_all("pipelines", query_parameters)
        return pipelines[0] if pipelines else None

    def load_release_context(
        self,
        version_text: str,
        validate_local: bool,
        target_branch: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        """Validate and reconstruct a release issue, MR, and target commit."""
        target_branch = target_branch or self.config.default_branch
        info(f"Reconstructing release context for {version_text}")
        CTAVersion.parse(version_text)

        if validate_local:
            self.git.validate_repository(
                target_branch,
                self.config.remote,
                fetch=not self.dry_run,
            )

        release_issue = self.find_or_create_release_issue(version_text, create=False)
        if release_issue is None:
            raise ReleaseWorkflowError(f"Release issue {self.config.issue_title(version_text)!r} does not exist")

        changelog_merge_request = self.find_changelog_merge_request(version_text, target_branch)
        if changelog_merge_request is None:
            raise ReleaseWorkflowError(f"Changelog MR for {version_text} does not exist")
        if (
            changelog_merge_request.get("state") != "merged"
            or changelog_merge_request.get("target_branch") != target_branch
        ):
            raise ReleaseWorkflowError(
                f"Changelog MR {changelog_merge_request.get('web_url')} has not been merged into {target_branch}"
            )

        release_commit = changelog_merge_request.get("squash_commit_sha") or changelog_merge_request.get(
            "merge_commit_sha"
        )
        if not release_commit:
            raise ReleaseWorkflowError(f"Changelog MR {changelog_merge_request.get('web_url')} has no resulting commit")

        containing_refs = self.api.get_all(f"repository/commits/{release_commit}/refs", {"type": "branch"})
        if not any(ref.get("name") == target_branch for ref in containing_refs):
            raise ReleaseWorkflowError(f"Release commit {release_commit} is not on {target_branch}")

        changelog, _ = self.read_repository_file(self.config.changelog_file, release_commit)
        if version_text.removeprefix("v") not in changelog:
            raise ReleaseWorkflowError(
                f"{self.config.changelog_file} at {release_commit} does not contain {version_text}"
            )

        return release_issue, changelog_merge_request, release_commit
