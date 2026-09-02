# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Shared Git and GitLab state used by release commands."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any
from urllib.parse import quote

from cta_version import CTAVersion, VersionError
from git_repo import Git
from gitlab_api import GitLabAPI
from release_config import ReleaseConfig

NOTE_MARKER = "<!-- cta-release:{version}:{stage} -->"


class ReleaseWorkflowError(RuntimeError):
    """A failure while coordinating an otherwise valid release workflow."""


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
        """Add one idempotent, stage-marked progress note to a release issue."""
        marker = NOTE_MARKER.format(version=version, stage=stage)

        if release_issue is None:
            if self.dry_run:
                print(f"DRY-RUN: add release issue note: {body}")
            return

        issue_iid = release_issue["iid"]
        existing_notes = self.api.get_all(f"issues/{issue_iid}/notes")
        if any(marker in note.get("body", "") for note in existing_notes):
            return

        if self.dry_run:
            print(f"DRY-RUN: add release issue note: {body}")
            return

        self.api.post(f"issues/{issue_iid}/notes", json={"body": f"{marker}\n{body}"})

    def find_changelog_merge_requests(self, version: str | None = None) -> list[dict[str, Any]]:
        """Find changelog merge requests, optionally restricted to one version."""
        query_parameters: dict[str, Any] = {
            "scope": "all",
            "target_branch": self.config.default_branch,
        }
        if version:
            query_parameters["source_branch"] = self.config.changelog_branch(version)
            return self.api.get_all("merge_requests", query_parameters)

        query_parameters.update(
            {
                "state": "merged",
                "labels": self.config.release_label,
                "order_by": "updated_at",
                "sort": "desc",
            }
        )
        return self.api.get_page(
            "merge_requests",
            query_parameters,
            per_page=self.config.release_discovery_limit,
        )

    def find_changelog_merge_request(self, version: str) -> dict[str, Any] | None:
        """Find the unique deterministic changelog merge request for a release."""
        matching_merge_requests = [
            changelog_merge_request
            for changelog_merge_request in self.find_changelog_merge_requests(version)
            if changelog_merge_request.get("source_branch") == self.config.changelog_branch(version)
            and changelog_merge_request.get("title") == self.config.changelog_merge_request_title(version)
        ]

        if len(matching_merge_requests) > 1:
            raise ReleaseWorkflowError(f"Multiple changelog MRs exist for {version}; resolve duplicates")

        return matching_merge_requests[0] if matching_merge_requests else None

    def discover_unfinished_release_version(self) -> str:
        """Infer the sole merged release whose base tag has not been pushed."""
        info("Searching GitLab for merged release MRs without a corresponding tag")
        candidate_versions: list[str] = []
        remote_tags = self.git.remote_tag_names(self.config.remote)

        for changelog_merge_request in self.find_changelog_merge_requests():
            source_branch = str(changelog_merge_request.get("source_branch", ""))
            if changelog_merge_request.get("state") != "merged" or not source_branch.endswith(
                self.config.branch_suffix
            ):
                continue

            version = source_branch[: -len(self.config.branch_suffix)]
            try:
                CTAVersion.parse(version, require_base=True)
            except VersionError:
                continue

            if version not in remote_tags:
                candidate_versions.append(version)

        candidate_versions = sorted(set(candidate_versions), key=lambda item: CTAVersion.parse(item).core)
        info(f"Found {len(candidate_versions)} unfinished release candidate(s)")

        if not candidate_versions:
            raise ReleaseWorkflowError("No merged, unfinished release merge request was found")
        if len(candidate_versions) > 1:
            candidate_listing = "\n".join(f"  {candidate}" for candidate in candidate_versions)
            raise ReleaseWorkflowError(
                f"Multiple unfinished releases were found:\n\n{candidate_listing}"
                f"\n\nRun:\nrelease tag {candidate_versions[-1]}"
            )

        print(f"Inferred release version: {candidate_versions[0]}")
        return candidate_versions[0]

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
        self, version_text: str, validate_local: bool
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        """Validate and reconstruct a release issue, MR, and target commit."""
        info(f"Reconstructing release context for {version_text}")
        CTAVersion.parse(version_text)

        if validate_local:
            self.git.validate_repository(
                self.config.default_branch,
                self.config.remote,
                fetch=not self.dry_run,
            )

        release_issue = self.find_or_create_release_issue(version_text, create=False)
        if release_issue is None:
            raise ReleaseWorkflowError(f"Release issue {self.config.issue_title(version_text)!r} does not exist")

        changelog_merge_request = self.find_changelog_merge_request(version_text)
        if changelog_merge_request is None:
            raise ReleaseWorkflowError(f"Changelog MR for {version_text} does not exist")
        if (
            changelog_merge_request.get("state") != "merged"
            or changelog_merge_request.get("target_branch") != self.config.default_branch
        ):
            raise ReleaseWorkflowError(
                f"Changelog MR {changelog_merge_request.get('web_url')} has not been merged into "
                f"{self.config.default_branch}"
            )

        release_commit = changelog_merge_request.get("squash_commit_sha") or changelog_merge_request.get(
            "merge_commit_sha"
        )
        if not release_commit:
            raise ReleaseWorkflowError(f"Changelog MR {changelog_merge_request.get('web_url')} has no resulting commit")

        containing_refs = self.api.get_all(f"repository/commits/{release_commit}/refs", {"type": "branch"})
        if not any(ref.get("name") == self.config.default_branch for ref in containing_refs):
            raise ReleaseWorkflowError(f"Release commit {release_commit} is not on {self.config.default_branch}")

        changelog, _ = self.read_repository_file(self.config.changelog_file, release_commit)
        if version_text.removeprefix("v") not in changelog:
            raise ReleaseWorkflowError(
                f"{self.config.changelog_file} at {release_commit} does not contain {version_text}"
            )

        return release_issue, changelog_merge_request, release_commit
