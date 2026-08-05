# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Repository-specific configuration for CTA release automation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReleaseConfig:
    """Centralize repository-specific constants for CTA releases."""

    gitlab_url: str = "https://gitlab.cern.ch"
    project_id: str = "139306"
    project_path: str = "cta/CTA"
    remote: str = "origin"
    default_branch: str = "main"
    changelog_file: str = "CHANGELOG.md"
    issue_template: str = ".gitlab/issue_templates/Release.md"
    release_label: str = "type::release"
    branch_suffix: str = "-changelog-update"
    # Prevent tag creation unless its target commit has a successful pipeline.
    require_successful_target_pipeline: bool = True

    def issue_title(self, version: str) -> str:
        """Return the deterministic release issue title."""
        return f"Release {version}"

    def changelog_merge_request_title(self, version: str) -> str:
        """Return the deterministic changelog merge request title."""
        return f"[Misc] Update changelog for release {version.removeprefix('v')}"

    def changelog_branch(self, version: str) -> str:
        """Return the deterministic changelog branch name."""
        return f"{version}{self.branch_suffix}"

    @property
    def project_web_url(self) -> str:
        """Return the human-facing GitLab project URL."""
        return f"{self.gitlab_url}/{self.project_path}"
