#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import base64
import re
import sys
import time
from typing import Any
from urllib.parse import quote

from gitlab_api import GitLabAPI, GitLabAPIError

PIPELINE_CONFIG_PATH = ".gitlab-ci.yml"
VERSION_PATTERN = re.compile(r'^( {2}PIPELINE_IMAGE_VERSION: ")[^"]+("\s*)$', re.MULTILINE)
MERGE_REQUEST_LABELS = ("priority::medium", "type::maintenance", "ci: pipeline")
MERGE_REQUEST_POLL_INTERVAL_SECONDS = 5
MERGE_REQUEST_READY_TIMEOUT_SECONDS = 300
TRANSIENT_MERGE_STATUSES = {"unchecked", "preparing", "checking", "approvals_syncing"}


class PipelineImageUpdateError(Exception):
    """Raised when the GitLab-side pipeline-image update cannot be completed safely."""


def log_task(message: str) -> None:
    """Log a pipeline-image update step in the same style as log_utils.sh."""
    print(f"==> {message}")


def replace_pipeline_image_version(content: str, version: str) -> str:
    """Replace the single committed pipeline-image release version."""
    updated, replacements = VERSION_PATTERN.subn(rf"\g<1>{version}\g<2>", content)
    if replacements != 1:
        raise ValueError(
            f"Expected exactly one PIPELINE_IMAGE_VERSION assignment in {PIPELINE_CONFIG_PATH}, found {replacements}"
        )
    return updated


def require_single_line_change(original: str, updated: str) -> None:
    """Ensure the resulting commit replaces exactly one line."""
    original_lines = original.splitlines(keepends=True)
    updated_lines = updated.splitlines(keepends=True)
    if len(original_lines) != len(updated_lines):
        raise ValueError(f"Expected the update to replace exactly one line in {PIPELINE_CONFIG_PATH}")

    changed_lines = sum(before != after for before, after in zip(original_lines, updated_lines))
    if changed_lines != 1:
        raise ValueError(
            f"Expected the update to replace exactly one line in {PIPELINE_CONFIG_PATH}, found {changed_lines}"
        )


def merge_request_title(version: str) -> str:
    return f'[CI] Update pipeline images to "{version}"'


def merge_request_description(version: str) -> str:
    return (
        "### Description\n\n"
        "Automated refresh of the complete pipeline-image set.\n\n"
        f"Shared images were published as {version} and platform-specific images as "
        f"{version}.<platform> before this merge request was created.\n\n"
        "### Checklist\n\n"
        "- [x] Documentation reflects the changes made.\n"
        "- [x] Merge Request title is clear, concise, and suitable as a changelog entry. "
        "See [this link](https://cta.docs.cern.ch/latest/dev/contributing/workflow/#changelog)"
    )


def decode_repository_file(file_metadata: dict[str, Any]) -> str:
    if file_metadata.get("encoding") != "base64" or not isinstance(file_metadata.get("content"), str):
        raise ValueError("GitLab returned repository file content in an unsupported format")
    return base64.b64decode(file_metadata["content"]).decode()


def get_repository_file(api: GitLabAPI, ref: str) -> dict[str, Any]:
    result = api.get(f"repository/files/{quote(PIPELINE_CONFIG_PATH, safe='')}", params={"ref": ref})
    if not isinstance(result, dict):
        raise PipelineImageUpdateError(f"Failed to read {PIPELINE_CONFIG_PATH} from {ref}")
    return result


def ensure_source_branch(api: GitLabAPI, source_branch: str, target_branch: str) -> None:
    log_task(f"Checking for source branch {source_branch}")
    branches = api.get("repository/branches", params={"search": f"^{re.escape(source_branch)}$"})
    if not isinstance(branches, list):
        raise PipelineImageUpdateError("Failed to query the pipeline-image update branch")
    if any(branch.get("name") == source_branch for branch in branches if isinstance(branch, dict)):
        log_task(f"Reusing existing source branch {source_branch}")
        return

    log_task(f"Creating source branch {source_branch} from {target_branch}")
    created = api.post("repository/branches", params={"branch": source_branch, "ref": target_branch})
    if not isinstance(created, dict):
        raise PipelineImageUpdateError(f"Failed to create branch {source_branch} from {target_branch}")


def update_version_file(
    api: GitLabAPI,
    source_branch: str,
    target_branch: str,
    version: str,
    commit_title: str,
) -> bool:
    log_task(f"Reading {PIPELINE_CONFIG_PATH} from {target_branch}")
    target_file = get_repository_file(api, target_branch)
    target_content = decode_repository_file(target_file)
    updated_content = replace_pipeline_image_version(target_content, version)
    log_task("Verifying that the generated update changes exactly one line")
    require_single_line_change(target_content, updated_content)

    ensure_source_branch(api, source_branch, target_branch)
    source_file = get_repository_file(api, source_branch)
    source_content = decode_repository_file(source_file)
    if source_content == updated_content:
        log_task(f"{PIPELINE_CONFIG_PATH} already selects pipeline image release {version}")
        return False
    require_single_line_change(source_content, updated_content)

    log_task(f"Committing pipeline image release {version} to {source_branch}")
    result = api.put(
        f"repository/files/{quote(PIPELINE_CONFIG_PATH, safe='')}",
        json={
            "branch": source_branch,
            "commit_message": commit_title,
            "content": updated_content,
            "last_commit_id": source_file.get("last_commit_id"),
        },
    )
    if not isinstance(result, dict):
        raise PipelineImageUpdateError(f"Failed to update {PIPELINE_CONFIG_PATH} on {source_branch}")
    return True


def create_or_refresh_merge_request(
    api: GitLabAPI,
    source_branch: str,
    target_branch: str,
    title: str,
    version: str,
    triggering_user_id: int,
) -> dict[str, Any]:
    description = merge_request_description(version)
    labels = ",".join(MERGE_REQUEST_LABELS)
    participants = {
        "assignee_ids": [triggering_user_id],
        "reviewer_ids": [triggering_user_id],
    }
    log_task(f"Checking for an existing merge request from {source_branch} to {target_branch}")
    merge_requests = api.get(
        "merge_requests",
        params={"state": "opened", "source_branch": source_branch, "target_branch": target_branch},
    )
    if not isinstance(merge_requests, list):
        raise PipelineImageUpdateError("Failed to query existing pipeline-image update merge requests")

    if merge_requests:
        merge_request = merge_requests[0]
        iid = merge_request.get("iid")
        if not isinstance(iid, int):
            raise PipelineImageUpdateError("Existing merge request has no valid IID")
        log_task(f"Refreshing existing merge request !{iid}")
        refreshed = api.put(
            f"merge_requests/{iid}",
            json={"title": title, "description": description, "labels": labels, **participants},
        )
        if not isinstance(refreshed, dict):
            raise PipelineImageUpdateError(f"Failed to refresh merge request !{iid}")
        if "web_url" not in refreshed and "web_url" in merge_request:
            refreshed["web_url"] = merge_request["web_url"]
        return refreshed

    log_task(f"Creating merge request from {source_branch} to {target_branch}")
    created = api.post(
        "merge_requests",
        json={
            "source_branch": source_branch,
            "target_branch": target_branch,
            "title": title,
            "description": description,
            "labels": labels,
            **participants,
            "remove_source_branch": True,
            "squash": True,
        },
    )
    if not isinstance(created, dict):
        raise PipelineImageUpdateError("Failed to create the pipeline-image update merge request")
    return created


def wait_for_merge_request_readiness(
    api: GitLabAPI,
    iid: int,
    expected_sha: str,
    *,
    timeout_seconds: int = MERGE_REQUEST_READY_TIMEOUT_SECONDS,
    poll_interval_seconds: int = MERGE_REQUEST_POLL_INTERVAL_SECONDS,
    additional_transient_statuses: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    attempts = max(1, timeout_seconds // max(1, poll_interval_seconds))
    last_status = "unknown"

    for attempt in range(attempts):
        merge_request = api.get(
            f"merge_requests/{iid}",
            params={"with_merge_status_recheck": "true"},
        )
        if not isinstance(merge_request, dict):
            raise PipelineImageUpdateError(f"Failed to query readiness of merge request !{iid}")
        if merge_request.get("state") != "opened":
            raise PipelineImageUpdateError(
                f"Merge request !{iid} is no longer open (state: {merge_request.get('state', 'unknown')})"
            )
        if merge_request.get("sha") != expected_sha:
            raise PipelineImageUpdateError(f"Merge request !{iid} source SHA changed while waiting")

        last_status = str(merge_request.get("detailed_merge_status", "unknown"))
        transient_statuses = TRANSIENT_MERGE_STATUSES | additional_transient_statuses
        if merge_request.get("prepared_at") and last_status not in transient_statuses:
            log_task(f"Merge request !{iid} is ready (status: {last_status})")
            return merge_request

        if attempt + 1 < attempts:
            log_task(f"Waiting for merge request !{iid} readiness (status: {last_status})")
            time.sleep(poll_interval_seconds)

    raise PipelineImageUpdateError(f"Timed out waiting for merge request !{iid} readiness (last status: {last_status})")


def wait_for_merge_request_approval(
    api: GitLabAPI,
    iid: int,
    *,
    timeout_seconds: int = MERGE_REQUEST_READY_TIMEOUT_SECONDS,
    poll_interval_seconds: int = MERGE_REQUEST_POLL_INTERVAL_SECONDS,
) -> None:
    attempts = max(1, timeout_seconds // max(1, poll_interval_seconds))

    for attempt in range(attempts):
        approval_state = api.get(f"merge_requests/{iid}/approval_state")
        if not isinstance(approval_state, dict):
            raise PipelineImageUpdateError(f"Failed to query approval state of merge request !{iid}")
        if approval_state.get("approved") is True:
            log_task(f"Merge request !{iid} satisfies its approval rules")
            return

        if attempt + 1 < attempts:
            log_task(f"Waiting for merge request !{iid} approval to become effective")
            time.sleep(poll_interval_seconds)

    raise PipelineImageUpdateError(f"Merge request !{iid} did not satisfy its approval rules before the timeout")


def approve_and_enable_auto_merge(api: GitLabAPI, merge_request: dict[str, Any]) -> None:
    iid = merge_request.get("iid")
    sha = merge_request.get("sha")
    if not isinstance(iid, int) or not isinstance(sha, str):
        raise PipelineImageUpdateError("Pipeline-image update merge request has no valid IID or SHA")

    log_task(f"Waiting for merge request !{iid} preparation")
    wait_for_merge_request_readiness(api, iid, sha)
    log_task(f"Approving merge request !{iid}")
    approved = api.post(f"merge_requests/{iid}/approve", json={"sha": sha})
    if not isinstance(approved, dict):
        raise PipelineImageUpdateError(f"Failed to approve merge request !{iid}")

    log_task(f"Verifying approval rules for merge request !{iid}")
    wait_for_merge_request_approval(api, iid)
    log_task(f"Waiting for merge request !{iid} status synchronization")
    merge_request = wait_for_merge_request_readiness(
        api,
        iid,
        sha,
        additional_transient_statuses=frozenset({"not_approved"}),
    )

    if merge_request.get("merge_when_pipeline_succeeds") is True:
        log_task(f"Merge request !{iid} already has auto-merge enabled")
        return

    log_task(f"Enabling auto-merge for merge request !{iid}")
    auto_merge = api.put(
        f"merge_requests/{iid}/merge",
        json={
            "auto_merge": True,
            "sha": sha,
            "should_remove_source_branch": True,
            "squash": True,
        },
    )
    if not isinstance(auto_merge, dict):
        raise PipelineImageUpdateError(f"Failed to enable auto-merge for merge request !{iid}")


def update_pipeline_image_release(
    api: GitLabAPI,
    source_branch: str,
    target_branch: str,
    version: str,
    triggering_user_id: int,
    auto_merge: bool,
) -> str:
    log_task(f"Starting pipeline image release update to {version}")
    title = merge_request_title(version)
    update_version_file(api, source_branch, target_branch, version, title)
    merge_request = create_or_refresh_merge_request(
        api, source_branch, target_branch, title, version, triggering_user_id
    )
    if auto_merge:
        approve_and_enable_auto_merge(api, merge_request)
    else:
        log_task("Automatic approval and merge are disabled for this pipeline ref")
    return str(merge_request.get("web_url", f"!{merge_request['iid']}"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update the pinned pipeline-image release and create or refresh its MR"
    )
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--api-token", required=True)
    parser.add_argument("--source-branch", required=True)
    parser.add_argument("--target-branch", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--triggering-user-id", required=True, type=int)
    parser.add_argument("--auto-merge", required=True, choices=("true", "false"))
    args = parser.parse_args()

    try:
        merge_request_url = update_pipeline_image_release(
            GitLabAPI(args.api_url, args.project_id, args.api_token),
            args.source_branch,
            args.target_branch,
            args.version,
            args.triggering_user_id,
            args.auto_merge == "true",
        )
    except (GitLabAPIError, PipelineImageUpdateError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    log_task(f"Pipeline-image update merge request: {merge_request_url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
