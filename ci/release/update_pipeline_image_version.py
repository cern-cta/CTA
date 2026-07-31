#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import base64
import re
import sys
from typing import Any
from urllib.parse import quote

from gitlabapi import GitLabAPI

PIPELINE_CONFIG_PATH = ".gitlab-ci.yml"
VERSION_PATTERN = re.compile(r'^(  PIPELINE_IMAGE_VERSION: ")[^"]+("\s*)$', re.MULTILINE)
SHARED_IMAGE_VARIABLES = ("IMAGE_DEFAULT", "IMAGE_LINT", "IMAGE_RELEASE", "IMAGE_DANGER")


class PipelineImageUpdateError(Exception):
    """Raised when the GitLab-side pipeline-image update cannot be completed safely."""


def replace_pipeline_image_version(content: str, version: str) -> str:
    """Replace the single committed pipeline-image release version."""
    updated, replacements = VERSION_PATTERN.subn(rf"\g<1>{version}\g<2>", content)
    if replacements != 1:
        raise ValueError(
            f"Expected exactly one PIPELINE_IMAGE_VERSION assignment in {PIPELINE_CONFIG_PATH}, found {replacements}"
        )
    return updated


def use_platform_agnostic_shared_image_tags(content: str) -> str:
    """Remove the legacy platform suffix from each shared pipeline-image reference."""
    updated = content
    for variable in SHARED_IMAGE_VARIABLES:
        pattern = re.compile(
            rf'^(  {variable}: "[^"]*:\$\{{PIPELINE_IMAGE_VERSION\}})(?:\.\$\{{PLATFORM\}})?("\s*)$',
            re.MULTILINE,
        )
        updated, replacements = pattern.subn(r"\g<1>\g<2>", updated)
        if replacements != 1:
            raise ValueError(
                f"Expected exactly one {variable} assignment in {PIPELINE_CONFIG_PATH}, found {replacements}"
            )
    return updated


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
    branches = api.get("repository/branches", params={"search": f"^{re.escape(source_branch)}$"})
    if not isinstance(branches, list):
        raise PipelineImageUpdateError("Failed to query the pipeline-image update branch")
    if any(branch.get("name") == source_branch for branch in branches if isinstance(branch, dict)):
        return

    created = api.post("repository/branches", params={"branch": source_branch, "ref": target_branch})
    if not isinstance(created, dict):
        raise PipelineImageUpdateError(f"Failed to create branch {source_branch} from {target_branch}")


def update_version_file(
    api: GitLabAPI,
    source_branch: str,
    target_branch: str,
    version: str,
) -> bool:
    target_file = get_repository_file(api, target_branch)
    updated_content = replace_pipeline_image_version(decode_repository_file(target_file), version)
    updated_content = use_platform_agnostic_shared_image_tags(updated_content)

    ensure_source_branch(api, source_branch, target_branch)
    source_file = get_repository_file(api, source_branch)
    if decode_repository_file(source_file) == updated_content:
        print(f"{PIPELINE_CONFIG_PATH} already selects pipeline image release {version}")
        return False

    result = api.put(
        f"repository/files/{quote(PIPELINE_CONFIG_PATH, safe='')}",
        json={
            "branch": source_branch,
            "commit_message": f'[CI] Update pipeline image version to "{version}"',
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
    version: str,
) -> str:
    title = f'[CI] Update pipeline images to "{version}"'
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
        refreshed = api.put(f"merge_requests/{iid}", json={"title": title})
        if not isinstance(refreshed, dict):
            raise PipelineImageUpdateError(f"Failed to refresh merge request !{iid}")
        return str(refreshed.get("web_url", merge_request.get("web_url", f"!{iid}")))

    created = api.post(
        "merge_requests",
        json={
            "source_branch": source_branch,
            "target_branch": target_branch,
            "title": title,
            "description": (
                "Automated weekly refresh of the complete pipeline-image set.\n\n"
                f"Shared images were published as `{version}` and platform-specific images as "
                f"`{version}.<platform>` before this merge request was created."
            ),
            "remove_source_branch": True,
            "squash": True,
        },
    )
    if not isinstance(created, dict):
        raise PipelineImageUpdateError("Failed to create the pipeline-image update merge request")
    return str(created.get("web_url", "created merge request"))


def update_pipeline_image_release(
    api: GitLabAPI,
    source_branch: str,
    target_branch: str,
    version: str,
) -> str:
    update_version_file(api, source_branch, target_branch, version)
    return create_or_refresh_merge_request(api, source_branch, target_branch, version)


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
    args = parser.parse_args()

    try:
        merge_request_url = update_pipeline_image_release(
            GitLabAPI(args.api_url, args.project_id, args.api_token),
            args.source_branch,
            args.target_branch,
            args.version,
        )
    except (PipelineImageUpdateError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(f"Pipeline-image update merge request: {merge_request_url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
