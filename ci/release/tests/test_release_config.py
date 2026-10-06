# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cta_version import VersionError
from git_repo import Git
from release_config import read_release_family, validate_release_family


def test_release_family_comes_from_selected_commit(tmp_path: Path) -> None:
    git = Git(tmp_path)
    git.run(["init"])
    git.run(["config", "user.name", "Release test"])
    git.run(["config", "user.email", "release-test@example.invalid"])
    project_file = tmp_path / "project.json"
    project_file.write_text('{"releaseFamily": "6"}', encoding="utf-8")
    git.run(["add", "project.json"])
    git.run(["commit", "-m", "Release target"])
    release_commit = git.run(["rev-parse", "HEAD"])

    # Both the newer checkout and uncommitted metadata differ from the release target.
    project_file.write_text('{"releaseFamily": "7"}', encoding="utf-8")
    git.run(["commit", "-am", "Next release family"])
    project_file.write_text('{"releaseFamily": "8"}', encoding="utf-8")

    validate_release_family(git, release_commit, "v6.12.0.0-1")
    assert read_release_family(git, "HEAD") == "7"
    with pytest.raises(VersionError, match="does not match releaseFamily '6'"):
        validate_release_family(git, release_commit, "v7.12.0.0-1")


@pytest.mark.parametrize("metadata", ["{}", '{"releaseFamily": 6}', '{"releaseFamily": "0"}', "invalid"])
def test_rejects_invalid_release_metadata(metadata: str) -> None:
    git = MagicMock(spec=Git)
    git.run.return_value = metadata
    with pytest.raises(VersionError, match="releaseFamily"):
        read_release_family(git, "release-commit")
