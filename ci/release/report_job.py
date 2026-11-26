# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Report one CI job result to the release issue."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from gitlab_api import GitLabAPI
from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError
from release_version import CTAVersion

RESULTS = {"success": "✅ Succeeded", "failed": "❌ Failed", "canceled": "⏹️ Canceled"}


def render_job(job: dict[str, Any], description: str) -> str:
    """Render a completed job; omitted, skipped, or unstarted jobs produce no report."""
    result = RESULTS.get(job["status"])
    if result is None:
        return ""
    return f"{result}: {description} — [{job['name']}]({job['web_url']})"


def publish_report(
    api: GitLabAPI,
    config: ReleaseConfig,
    tag: str,
    pipeline_id: str,
    pipeline_url: str,
    report: str,
    job_name: str,
) -> None:
    """Update one job result in the pipeline thread without replacing other results or replies."""
    release = CTAVersion.parse(tag)
    version = CTAVersion(*release.core).text
    issues = ReleaseContext(Path.cwd(), config, api, dry_run=False).find_release_issues(version)
    # Prereleases may precede the release issue and changelog.
    if not issues and (release.development or release.release_candidate):
        return
    if len(issues) != 1:
        raise ReleaseWorkflowError(
            f"Expected one release issue titled {config.issue_title(version)!r}, found {len(issues)}"
        )

    endpoint = f"issues/{issues[0]['iid']}/discussions"
    pipeline_marker = f"<!-- cta-release-pipeline:{pipeline_id} -->"
    job_marker = f"<!-- cta-release-job:{pipeline_id}:{job_name} -->"
    body = f"{report}\n\n{job_marker}"
    # The CI resource group prevents concurrent reporters from creating duplicate pipeline threads.
    discussion = next(
        (
            item
            for item in api.get_all(endpoint)
            if any(pipeline_marker in note.get("body", "") for note in item.get("notes", []))
        ),
        None,
    )
    if discussion is None:
        heading = f"### Release pipeline for `{tag}`\n\n[View pipeline]({pipeline_url})\n\n{pipeline_marker}"
        discussion = api.post(endpoint, json={"body": heading})
    notes_endpoint = f"{endpoint}/{discussion['id']}/notes"
    for note in discussion.get("notes", []):
        if job_marker in note.get("body", ""):
            api.put(f"{notes_endpoint}/{note['id']}", json={"body": body})
            return
    api.post(notes_endpoint, json={"body": body})


def required_environment(name: str) -> str:
    """Read a required CI variable with an actionable error."""
    value = os.environ.get(name)
    if not value:
        raise ReleaseWorkflowError(f"Required CI variable {name} is not set")
    return value


def main(argv: list[str] | None = None) -> int:
    """Read the source job's latest attempt and update its release discussion note."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", required=True, help="Exact CI job name, including matrix values")
    parser.add_argument("--description", required=True, help="Short description of the reported work")
    args = parser.parse_args(argv)
    try:
        tag = required_environment("CI_COMMIT_TAG")
        pipeline_id = required_environment("CI_PIPELINE_ID")
        config = ReleaseConfig(
            gitlab_url=os.environ.get("CI_SERVER_URL", ReleaseConfig.gitlab_url),
            project_id=os.environ.get("CI_PROJECT_ID", ReleaseConfig.project_id),
            project_path=os.environ.get("CI_PROJECT_PATH", ReleaseConfig.project_path),
        )
        api = GitLabAPI(config.gitlab_url, config.project_id, required_environment("CI_API_TOKEN"))
        jobs = api.get_all(f"pipelines/{pipeline_id}/jobs", params={"include_retried": "false"})
        job = next((item for item in jobs if item["name"] == args.job), None)
        if job is None:
            return 0
        report = render_job(job, args.description)
        if report:
            # Identify the platform/backend variant without depending on a producer's artifacts.
            if pipeline_name := os.environ.get("CI_PIPELINE_NAME"):
                report = f"**{pipeline_name}**\n\n{report}"
            publish_report(api, config, tag, pipeline_id, required_environment("CI_PIPELINE_URL"), report, args.job)
    except Exception as error:
        # CI marks this job allow_failure; keep errors visible without failing publication.
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
