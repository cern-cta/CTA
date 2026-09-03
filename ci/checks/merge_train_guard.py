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


def api_get(*path_segments: str, query: dict[str, str | int] | None = None) -> tuple[Any, dict[str, str], str]:
    base_url = os.environ["CI_API_V4_URL"].rstrip("/")
    project_id = urllib.parse.quote(os.environ["CI_PROJECT_ID"], safe="")
    encoded_path = "/".join(urllib.parse.quote(segment, safe="") for segment in path_segments)
    url = f"{base_url}/projects/{project_id}/{encoded_path}"
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"
    credentials = [
        ("job_token", "JOB-TOKEN", os.environ.get("CI_JOB_TOKEN", "")),
        ("project_token", "PRIVATE-TOKEN", os.environ.get("MERGE_TRAIN_READ_API_TOKEN", "")),
    ]
    last_error: Exception | None = None
    for authentication, header, token in credentials:
        if not token:
            continue
        request = urllib.request.Request(  # noqa: S310
            url,
            headers={header: token},
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:  # noqa: S310
                headers = {key.lower(): value for key, value in response.headers.items()}
                return json.load(response), headers, authentication
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
    raise RuntimeError(f"GitLab API request failed: {last_error}")


def is_duplicate() -> tuple[bool, str]:  # noqa: PLR0911, PLR0912
    # A safe skip requires two independent facts:
    # the train commit is based directly on the current target HEAD, and the same synthetic commit already passed.
    if os.environ.get("CI_MERGE_REQUEST_EVENT_TYPE") != "merge_train":
        return False, "not_merge_train"

    # Missing context must always result in running the full pipeline.
    # Guessing here could incorrectly reuse a pipeline from another MR, commit, or target branch.
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

    # Resolve the target through the API rather than a local remote-tracking ref, which may be stale in a shallow clone.
    target_branch = os.environ["CI_MERGE_REQUEST_TARGET_BRANCH_NAME"]
    branch, _, authentication = api_get("repository", "branches", target_branch)
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

    # MR pipeline history contains detached, merged-results, and merge-train pipelines.
    # Ref, source, status, and commit checks below distinguish the reusable merged-results pipelines.
    mr_iid = os.environ["CI_MERGE_REQUEST_IID"]
    pipelines, headers, authentication = api_get("merge_requests", mr_iid, "pipelines", query={"per_page": 100})
    if not isinstance(pipelines, list):
        return False, "malformed_pipeline_response"
    pipeline_history_paginated = bool(headers.get("x-next-page", ""))

    # GitLab regenerates synthetic commits, so equivalent merged-results and train commits can have different SHAs.
    # Compare their target parent and resulting trees instead.
    print(f"Checking {len(pipelines)} merge request pipelines for a successful equivalent run.")
    current_pipeline_id = os.environ["CI_PIPELINE_ID"]
    merged_results_ref = f"refs/merge-requests/{os.environ['CI_MERGE_REQUEST_IID']}/merge"
    # Only successful pipelines on GitLab's merged-results ref are eligible.
    # The current train pipeline is excluded explicitly even though it normally uses the /train ref.
    candidates = [
        pipeline
        for pipeline in pipelines
        if str(pipeline.get("id", "")) != current_pipeline_id
        and pipeline.get("status") == "success"
        and pipeline.get("source") == "merge_request_event"
        and pipeline.get("ref") == merged_results_ref
        and isinstance(pipeline.get("sha"), str)
    ]

    match = None
    for candidate in candidates:
        candidate_sha = candidate["sha"]
        # The candidate must have been built directly on the same target HEAD as the current train commit.
        # This rejects a successful merged-results pipeline created before the target branch advanced.
        candidate_commit, _, authentication = api_get("repository", "commits", candidate_sha)
        candidate_parents = candidate_commit.get("parent_ids", [])
        print(
            f"Comparing merged-results pipeline {candidate.get('id')} at {candidate_sha}; "
            f"parents: {', '.join(candidate_parents) if candidate_parents else 'none'}"
        )
        if target_sha not in candidate_parents:
            print("Candidate was not based directly on the current target HEAD.")
            continue

        # A straight comparison checks the two resulting repository trees without following merge ancestry.
        # Zero diffs means the earlier pipeline tested exactly the content that the train would test again.
        comparison, _, authentication = api_get(
            "repository",
            "compare",
            query={"from": candidate_sha, "to": commit_sha, "straight": "true"},
        )
        diffs = comparison.get("diffs")
        if comparison.get("compare_timeout") or not isinstance(diffs, list):
            print("Candidate comparison was incomplete or malformed.")
            continue
        print(f"Candidate differs from the train commit in {len(diffs)} file(s).")
        if not diffs:
            match = candidate
            break

    if match is None:
        # A match in this page is conclusive, regardless of whether older pages exist.
        # Without a match, however, an unsearched page might contain the equivalent successful pipeline.
        if pipeline_history_paginated:
            print("No match was found on the first page, and older pipeline history was not searched.")
            return False, "pipeline_history_paginated"

        # Print the complete bounded response so unexpected GitLab ref or response behavior is visible in the job log.
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
