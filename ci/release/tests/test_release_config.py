# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cta_version import VersionError
from git_repo import Git
from release_config import read_major_version, validate_major_version


def test_major_version_comes_from_selected_commit(tmp_path: Path) -> None:
    git = Git(tmp_path)
    git.run(["init"])
    git.run(["config", "user.name", "Release test"])
    git.run(["config", "user.email", "release-test@example.invalid"])
    project_file = tmp_path / "project.json"
    project_file.write_text('{"majorVersion": 6}', encoding="utf-8")
    git.run(["add", "project.json"])
    git.run(["commit", "-m", "Release target"])
    release_commit = git.run(["rev-parse", "HEAD"])

    # Both the newer checkout and uncommitted metadata differ from the release target.
    project_file.write_text('{"majorVersion": 7}', encoding="utf-8")
    git.run(["commit", "-am", "Next major version"])
    project_file.write_text('{"majorVersion": 8}', encoding="utf-8")

    validate_major_version(git, release_commit, "v6.12.0-1")
    assert read_major_version(git, "HEAD") == 7
    with pytest.raises(VersionError, match="does not match majorVersion 6"):
        validate_major_version(git, release_commit, "v7.12.0-1")


@pytest.mark.parametrize(
    "metadata",
    [
        "{}",
        '{"majorVersion": "6"}',
        '{"majorVersion": 0}',
        '{"majorVersion": -1}',
        '{"majorVersion": 1.5}',
        '{"majorVersion": true}',
        '{"majorVersion": null}',
        "invalid",
    ],
)
def test_rejects_invalid_release_metadata(metadata: str) -> None:
    git = MagicMock(spec=Git)
    git.run.return_value = metadata
    with pytest.raises(VersionError, match="majorVersion"):
        read_major_version(git, "release-commit")


@pytest.mark.parametrize(
    ("heading", "expected"),
    [
        ("## 6.12.0-1", True),
        ("## 6.12.0-1 (2026-10-28)", True),
        ("## 6.12.0-10", False),
        ("## 6.12.0-1.rc1", False),
        ("## 16.12.0-1", False),
        ("### 6.12.0-1", False),
        ("Mention 6.12.0-1", False),
    ],
)
def test_release_heading_matches_complete_version(heading: str, expected: bool) -> None:
    from release_config import has_release_heading

    assert has_release_heading(heading, "v6.12.0-1") is expected
