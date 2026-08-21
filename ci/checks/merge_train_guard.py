# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Determine whether a merge-train pipeline duplicates a successful merged-results pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def api_get(path: str) -> tuple[Any, dict[str, str], str]:
    base_url = os.environ["CI_API_V4_URL"].rstrip("/")
    project_id = urllib.parse.quote(os.environ["CI_PROJECT_ID"], safe="")
    credentials = [
        ("job_token", "JOB-TOKEN", os.environ.get("CI_JOB_TOKEN", "")),
        ("project_token", "PRIVATE-TOKEN", os.environ.get("MERGE_TRAIN_READ_API_TOKEN", "")),
    ]
    last_error: Exception | None = None
    for authentication, header, token in credentials:
        if not token:
            continue
        request = urllib.request.Request(  # noqa: S310
            f"{base_url}/projects/{project_id}/{path}",
            headers={header: token},
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:  # noqa: S310
                headers = {key.lower(): value for key, value in response.headers.items()}
                return json.load(response), headers, authentication
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
    raise RuntimeError(f"GitLab API request failed: {last_error}")


def is_duplicate() -> tuple[bool, str]:  # noqa: PLR0911
    if os.environ.get("CI_MERGE_REQUEST_EVENT_TYPE") != "merge_train":
        return False, "not_merge_train"

    required = [
        "CI_API_V4_URL",
        "CI_PROJECT_ID",
        "CI_MERGE_REQUEST_IID",
        "CI_MERGE_REQUEST_TARGET_BRANCH_NAME",
        "CI_COMMIT_SHA",
        "CI_PIPELINE_ID",
    ]
    if any(not os.environ.get(name) for name in required):
        return False, "missing_environment"

    target_branch = urllib.parse.quote(os.environ["CI_MERGE_REQUEST_TARGET_BRANCH_NAME"], safe="")
    branch, _, authentication = api_get(f"repository/branches/{target_branch}")
    target_sha = str(branch["commit"]["id"])

    commit_sha = os.environ["CI_COMMIT_SHA"]
    parents = subprocess.run(  # noqa: S603
        ["/usr/bin/git", "rev-list", "--parents", "-n", "1", commit_sha],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    if len(parents) != 3 or parents[1] != target_sha:
        return False, "target_advanced_or_train_not_empty"

    mr_iid = urllib.parse.quote(os.environ["CI_MERGE_REQUEST_IID"], safe="")
    pipelines, headers, authentication = api_get(f"merge_requests/{mr_iid}/pipelines?per_page=100")
    if headers.get("x-next-page", ""):
        return False, "pipeline_history_paginated"
    if not isinstance(pipelines, list):
        return False, "malformed_pipeline_response"

    current_pipeline_id = os.environ["CI_PIPELINE_ID"]
    merged_results_ref = f"refs/merge-requests/{os.environ['CI_MERGE_REQUEST_IID']}/merge"
    match = next(
        (
            pipeline
            for pipeline in pipelines
            if str(pipeline.get("id", "")) != current_pipeline_id
            and pipeline.get("status") == "success"
            and pipeline.get("source") == "merge_request_event"
            and pipeline.get("ref") == merged_results_ref
            and pipeline.get("sha") == commit_sha
            and str(pipeline.get("name", "")).startswith("event:merged_result - ")
        ),
        None,
    )
    if not match:
        return False, "no_matching_successful_pipeline"

    print(
        f"Equivalent successful pipeline found: {match.get('web_url', match.get('id'))} "
        f"(authentication: {authentication}, target: {target_sha})"
    )
    return True, "equivalent_successful_pipeline"


def main() -> int:
    try:
        duplicate, reason = is_duplicate()
    except (KeyError, RuntimeError, subprocess.SubprocessError, TypeError, ValueError) as error:
        print(f"Merge-train guard was inconclusive; running the job: {error}", file=sys.stderr)
        duplicate = False
        reason = f"inconclusive:{type(error).__name__}"
    print(f"Merge-train guard decision: {'skip' if duplicate else 'run'} ({reason})")
    output_path = os.environ.get("MERGE_TRAIN_DOTENV", "merge-train.env")
    with open(output_path, "w") as output:
        output.write(f"MERGE_TRAIN_DUPLICATE={'true' if duplicate else 'false'}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
