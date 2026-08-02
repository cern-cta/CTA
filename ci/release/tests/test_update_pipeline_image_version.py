# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import base64
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

RELEASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RELEASE_DIR))

from gitlab_api import GitLabAPIError  # noqa: E402
from update_pipeline_image_version import (  # noqa: E402
    get_repository_file,
    main,
    update_version_file,
)


def test_repository_reads_use_new_gitlab_api_params_interface() -> None:
    api = MagicMock()
    api.get.return_value = {"encoding": "base64", "content": "", "last_commit_id": "abc"}

    assert get_repository_file(api, "main") == api.get.return_value
    api.get.assert_called_once_with("repository/files/.gitlab-ci.yml", params={"ref": "main"})


def test_file_updates_use_new_gitlab_api_json_interface() -> None:
    api = MagicMock()
    api.put.return_value = {"file_path": ".gitlab-ci.yml", "branch": "update-images"}
    target = 'variables:\n  PIPELINE_IMAGE_VERSION: "old"\n'
    source = 'variables:\n  PIPELINE_IMAGE_VERSION: "older"\n'
    api.get.side_effect = [
        {"encoding": "base64", "content": base64.b64encode(target.encode()).decode()},
        [{"name": "update-images"}],
        {
            "encoding": "base64",
            "content": base64.b64encode(source.encode()).decode(),
            "last_commit_id": "abc",
        },
    ]

    assert update_version_file(api, "update-images", "main", "new", "Update images") is True
    api.put.assert_called_once_with(
        "repository/files/.gitlab-ci.yml",
        json={
            "branch": "update-images",
            "commit_message": "Update images",
            "content": 'variables:\n  PIPELINE_IMAGE_VERSION: "new"\n',
            "last_commit_id": "abc",
        },
    )


def test_main_reports_new_gitlab_api_errors_without_traceback(capsys: pytest.CaptureFixture[str]) -> None:
    arguments = [
        "update_pipeline_image_version.py",
        "--api-url",
        "https://gitlab.example",
        "--project-id",
        "group/project",
        "--api-token",
        "secret",
        "--source-branch",
        "update-images",
        "--target-branch",
        "main",
        "--version",
        "v1",
        "--triggering-user-id",
        "42",
        "--auto-merge",
        "false",
    ]
    with (
        patch.object(sys, "argv", arguments),
        patch("update_pipeline_image_version.update_pipeline_image_release", side_effect=GitLabAPIError("failed")),
    ):
        assert main() == 1

    captured = capsys.readouterr()
    assert captured.err == "ERROR: failed\n"
