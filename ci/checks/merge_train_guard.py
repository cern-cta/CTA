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
    # A safe skip requires two independent facts:
    # the train commit is based directly on the current target HEAD, and the same synthetic commit already passed.
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

    # GitLab can represent the synthetic result as either a single-parent commit or a two-parent merge commit.
    # This depends on the merge strategy (fast forward merges vs merge commits)
    # In both cases, a direct parent matching the current target HEAD proves this is the first entry
    # in an unchanged train.
    commit_sha = os.environ["CI_COMMIT_SHA"]
    commit_info = subprocess.run(  # noqa: S603
        ["/usr/bin/git", "show", "--no-patch", "--format=%H%n%P%n%s", commit_sha],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    if len(commit_info) < 3:
        return False, "malformed_synthetic_commit"

    synthetic_sha = commit_info[0]
    parents = commit_info[1].split()
    subject = " ".join(commit_info[2:])
    print(f"Synthetic commit: {synthetic_sha} ({subject})")
    print(f"Synthetic parents: {', '.join(parents) if parents else 'none'}")
    print(f"Current target: {os.environ['CI_MERGE_REQUEST_TARGET_BRANCH_NAME']} at {target_sha}")

    if len(parents) not in {1, 2}:
        print(f"Expected one or two synthetic commit parents, found {len(parents)}.")
        return False, "synthetic_commit_has_unexpected_parent_count"
    if target_sha not in parents:
        print("The current target HEAD is not a direct parent of the synthetic commit.")
        return False, "target_advanced_or_train_not_empty"

    print("The current target HEAD is a direct parent of the synthetic commit.")

    mr_iid = urllib.parse.quote(os.environ["CI_MERGE_REQUEST_IID"], safe="")
    pipelines, headers, authentication = api_get(f"merge_requests/{mr_iid}/pipelines?per_page=100")
    if headers.get("x-next-page", ""):
        return False, "pipeline_history_paginated"
    if not isinstance(pipelines, list):
        return False, "malformed_pipeline_response"

    # Reuse a result only when an earlier successful MR pipeline ran on this exact synthetic commit.
    print(f"Checking {len(pipelines)} merge request pipelines for a successful equivalent run.")
    current_pipeline_id = os.environ["CI_PIPELINE_ID"]
    match = next(
        (
            pipeline
            for pipeline in pipelines
            if str(pipeline.get("id", "")) != current_pipeline_id
            and pipeline.get("status") == "success"
            and pipeline.get("source") == "merge_request_event"
            and pipeline.get("sha") == commit_sha
        ),
        None,
    )
    if not match:
        for pipeline in pipelines:
            print(
                "Pipeline candidate: "
                f"id={pipeline.get('id')} status={pipeline.get('status')} source={pipeline.get('source')} "
                f"sha={pipeline.get('sha')} ref={pipeline.get('ref')} name={pipeline.get('name')}"
            )
        return False, "no_matching_successful_pipeline"

    print(
        f"Equivalent successful pipeline found: {match.get('web_url', match.get('id'))} "
        f"(authentication: {authentication}, target: {target_sha})"
    )
    return True, "equivalent_successful_pipeline"


def main() -> int:
    # Any missing or unexpected information fails safe by publishing a "run" decision to downstream jobs.
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
