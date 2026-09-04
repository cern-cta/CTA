#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Add a CI job progress note to the release issue for a CTA tag."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from cta_version import CTAVersion
from gitlab_api import GitLabAPI
from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError


def release_version_for_tag(tag: str) -> str:
    """Return the unsuffixed release version associated with a CI tag."""
    version = CTAVersion.parse(tag)
    return CTAVersion(*version.core).text


def discussion_marker(pipeline_id: str) -> str:
    """Return the hidden marker identifying one tag pipeline's discussion."""
    return f"<!-- cta-release-pipeline:{pipeline_id} -->"


def add_release_note(
    api: GitLabAPI,
    config: ReleaseConfig,
    tag: str,
    pipeline_id: str,
    pipeline_url: str,
    note: str,
) -> None:
    """Find the release issue and append a note to this pipeline's discussion."""
    version = release_version_for_tag(tag)
    context = ReleaseContext(Path.cwd(), config, api, dry_run=False)
    release_issues = context.find_release_issues(version)
    if len(release_issues) != 1:
        raise ReleaseWorkflowError(
            f"Expected one release issue titled {config.issue_title(version)!r}, found {len(release_issues)}"
        )

    issue_iid = release_issues[0]["iid"]
    discussions_endpoint = f"issues/{issue_iid}/discussions"
    marker = discussion_marker(pipeline_id)
    discussions: list[dict[str, Any]] = api.get_all(discussions_endpoint)

    for discussion in discussions:
        if any(marker in item.get("body", "") for item in discussion.get("notes", [])):
            api.post(f"{discussions_endpoint}/{discussion['id']}/notes", json={"body": note})
            return

    heading = f"### Release pipeline for `{tag}`\n\n[View pipeline]({pipeline_url})\n\n{note}\n\n{marker}"
    api.post(discussions_endpoint, json={"body": heading})


def required_environment(name: str) -> str:
    """Read a required CI variable with an actionable error."""
    value = os.environ.get(name)
    if not value:
        raise ReleaseWorkflowError(f"Required CI variable {name} is not set")
    return value


def main(argv: list[str] | None = None) -> int:
    """Parse a custom note and publish it using CI-provided release context."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-token", required=True, help="GitLab API token")
    parser.add_argument("note", help="Markdown note to add to the release pipeline discussion")
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        return 0

    try:
        config = ReleaseConfig(
            gitlab_url=os.environ.get("CI_SERVER_URL", ReleaseConfig.gitlab_url),
            project_id=os.environ.get("CI_PROJECT_ID", ReleaseConfig.project_id),
            project_path=os.environ.get("CI_PROJECT_PATH", ReleaseConfig.project_path),
        )
        api = GitLabAPI(config.gitlab_url, config.project_id, args.api_token)
        add_release_note(
            api,
            config,
            required_environment("CI_COMMIT_TAG"),
            required_environment("CI_PIPELINE_ID"),
            required_environment("CI_PIPELINE_URL"),
            args.note,
        )
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        # We don't want issue notes to fail a CI job, so this script is best-effort
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
