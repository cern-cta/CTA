# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import base64
import sys
from pathlib import Path
from typing import Any, Optional

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from update_pipeline_image_version import (
    replace_pipeline_image_version,
    update_pipeline_image_release,
    use_platform_agnostic_shared_image_tags,
)


def file_metadata(content: str, last_commit_id: str = "commit") -> dict[str, Any]:
    return {
        "content": base64.b64encode(content.encode()).decode(),
        "encoding": "base64",
        "last_commit_id": last_commit_id,
    }


class FakeGitLabAPI:
    def __init__(self, target_content: str, source_content: Optional[str] = None, existing_mr: bool = False) -> None:
        self.target_content = target_content
        self.source_content = source_content
        self.existing_mr = existing_mr
        self.file_updates: list[dict[str, Any]] = []
        self.mr_updates: list[dict[str, Any]] = []
        self.mr_creates: list[dict[str, Any]] = []

    def get(self, endpoint: str, params: Optional[dict[str, str]] = None) -> Any:
        params = params or {}
        if endpoint == "repository/branches":
            return [{"name": "ci/update-pipeline-images"}] if self.source_content is not None else []
        if endpoint.startswith("repository/files/"):
            content = self.target_content if params["ref"] == "main" else self.source_content
            if content is None:
                raise AssertionError("Source branch must exist before its file is read")
            return file_metadata(content)
        if endpoint == "merge_requests":
            if self.existing_mr:
                return [{"iid": 42, "web_url": "https://gitlab.example/mr/42"}]
            return []
        raise AssertionError(f"Unexpected GET endpoint: {endpoint}")

    def post(
        self,
        endpoint: str,
        params: Optional[dict[str, str]] = None,
        data: Any = None,
        json: Any = None,
    ) -> Any:
        del data
        if endpoint == "repository/branches":
            self.source_content = self.target_content
            return {"name": params["branch"] if params else ""}
        if endpoint == "merge_requests":
            self.mr_creates.append(json)
            return {"iid": 43, "web_url": "https://gitlab.example/mr/43"}
        raise AssertionError(f"Unexpected POST endpoint: {endpoint}")

    def put(
        self,
        endpoint: str,
        params: Optional[dict[str, str]] = None,
        data: Any = None,
        json: Any = None,
    ) -> Any:
        del params, data
        if endpoint.startswith("repository/files/"):
            self.file_updates.append(json)
            self.source_content = json["content"]
            return {"file_path": ".gitlab-ci.yml"}
        if endpoint.startswith("merge_requests/"):
            self.mr_updates.append(json)
            return {"iid": 42, "web_url": "https://gitlab.example/mr/42"}
        raise AssertionError(f"Unexpected PUT endpoint: {endpoint}")


CONFIG = (
    "variables:\n"
    '  PIPELINE_IMAGE_VERSION: "old-version"\n'
    '  IMAGE_DEFAULT: "registry/default:${PIPELINE_IMAGE_VERSION}.${PLATFORM}"\n'
    '  IMAGE_LINT: "registry/lint:${PIPELINE_IMAGE_VERSION}.${PLATFORM}"\n'
    '  IMAGE_TEST: "registry/image:${PIPELINE_IMAGE_VERSION}.${PLATFORM}"\n'
    '  IMAGE_RELEASE: "registry/release:${PIPELINE_IMAGE_VERSION}.${PLATFORM}"\n'
    '  IMAGE_DANGER: "registry/danger:${PIPELINE_IMAGE_VERSION}.${PLATFORM}"\n'
)


def test_replace_changes_only_the_release_version() -> None:
    updated = replace_pipeline_image_version(CONFIG, "2026-07-31-12345")
    assert updated == CONFIG.replace('"old-version"', '"2026-07-31-12345"')


def test_replace_requires_exactly_one_assignment() -> None:
    with pytest.raises(ValueError, match="found 0"):
        replace_pipeline_image_version("variables:\n", "new-version")
    with pytest.raises(ValueError, match="found 2"):
        replace_pipeline_image_version(CONFIG + CONFIG, "new-version")


def test_shared_images_become_platform_agnostic() -> None:
    updated = use_platform_agnostic_shared_image_tags(CONFIG)

    assert 'IMAGE_DEFAULT: "registry/default:${PIPELINE_IMAGE_VERSION}"' in updated
    assert 'IMAGE_LINT: "registry/lint:${PIPELINE_IMAGE_VERSION}"' in updated
    assert 'IMAGE_RELEASE: "registry/release:${PIPELINE_IMAGE_VERSION}"' in updated
    assert 'IMAGE_DANGER: "registry/danger:${PIPELINE_IMAGE_VERSION}"' in updated
    assert 'IMAGE_TEST: "registry/image:${PIPELINE_IMAGE_VERSION}.${PLATFORM}"' in updated


def test_shared_image_migration_is_idempotent() -> None:
    migrated = use_platform_agnostic_shared_image_tags(CONFIG)
    assert use_platform_agnostic_shared_image_tags(migrated) == migrated


def test_creates_branch_commit_and_merge_request() -> None:
    api = FakeGitLabAPI(CONFIG)
    url = update_pipeline_image_release(api, "ci/update-pipeline-images", "main", "new-version")

    assert url == "https://gitlab.example/mr/43"
    assert len(api.file_updates) == 1
    assert 'PIPELINE_IMAGE_VERSION: "new-version"' in api.file_updates[0]["content"]
    assert 'IMAGE_DEFAULT: "registry/default:${PIPELINE_IMAGE_VERSION}"' in api.file_updates[0]["content"]
    assert len(api.mr_creates) == 1


def test_refreshes_existing_merge_request() -> None:
    api = FakeGitLabAPI(CONFIG, CONFIG, existing_mr=True)
    url = update_pipeline_image_release(api, "ci/update-pipeline-images", "main", "new-version")

    assert url == "https://gitlab.example/mr/42"
    assert len(api.file_updates) == 1
    assert api.mr_updates == [{"title": '[CI] Update pipeline images to "new-version"'}]
    assert api.mr_creates == []


def test_repeated_update_is_idempotent() -> None:
    selected = replace_pipeline_image_version(CONFIG, "new-version")
    selected = use_platform_agnostic_shared_image_tags(selected)
    api = FakeGitLabAPI(CONFIG, selected, existing_mr=True)
    update_pipeline_image_release(api, "ci/update-pipeline-images", "main", "new-version")

    assert api.file_updates == []
    assert len(api.mr_updates) == 1
