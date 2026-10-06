# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Repository-specific configuration for CTA release automation."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from cta_version import CTAVersion, VersionError
from git_repo import Git, GitError


def read_release_family(git: Git, revision: str) -> str:
    """Read release policy from the selected commit, ignoring the working tree."""
    project_file = f"{revision}:project.json"
    try:
        project = json.loads(git.run(["show", project_file]))
    except (GitError, ValueError) as error:
        raise VersionError(f"Could not read releaseFamily from {project_file}: {error}") from error
    family = project.get("releaseFamily") if isinstance(project, dict) else None
    if not isinstance(family, str) or re.fullmatch(r"[1-9]\d*", family, flags=re.ASCII) is None:
        raise VersionError(f"Invalid or missing releaseFamily in {project_file}; expected a positive integer string")
    return family


def validate_release_family(git: Git, revision: str, version: str) -> None:
    """Require a new release to belong to the configured family."""
    release_version = CTAVersion.parse(version, require_base=True)
    release_family = read_release_family(git, revision)
    if str(release_version.xrootd) != release_family:
        raise VersionError(
            f"Release {release_version.text} does not match releaseFamily {release_family!r} in {revision}:project.json"
        )


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

    def changelog_branch(self, version: str, target_branch: str) -> str:
        """Return the deterministic changelog branch name."""
        target_slug = re.sub(r"[^A-Za-z0-9._-]+", "-", target_branch).strip(".-")
        return f"{version}-{target_slug}{self.branch_suffix}"

    @property
    def project_web_url(self) -> str:
        """Return the human-facing GitLab project URL."""
        return f"{self.gitlab_url}/{self.project_path}"
