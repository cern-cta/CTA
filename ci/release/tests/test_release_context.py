# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typing_extensions import override

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError


class ReleaseContextTest(unittest.TestCase):
    """Test operations shared by multiple release commands."""

    @override
    def setUp(self) -> None:
        self.api = MagicMock()
        self.context = ReleaseContext(Path("/tmp"), ReleaseConfig(), self.api, dry_run=False)

    def test_closed_changelog_merge_requests_are_ignored(self) -> None:
        source_branch = "v5.12.0.0-1-main-changelog-update"
        self.api.get_all.return_value = [
            {"state": "closed", "target_branch": "main", "source_branch": source_branch},
        ]
        assert self.context.find_changelog_merge_request("v5.12.0.0-1", "main") is None

    def test_target_branch_is_part_of_changelog_branch(self) -> None:
        assert self.context.config.changelog_branch("v5.12.0.0-1", "release/5.12") == (
            "v5.12.0.0-1-release-5.12-changelog-update"
        )

    def test_active_source_branch_collision_is_rejected(self) -> None:
        self.api.get_all.return_value = [
            {
                "state": "opened",
                "title": "[Misc] Update changelog for release 5.12.0.0-1",
                "target_branch": "other",
                "source_branch": "v5.12.0.0-1-main-changelog-update",
            }
        ]
        with pytest.raises(ReleaseWorkflowError, match="targeting another branch"):
            self.context.find_changelog_merge_request("v5.12.0.0-1", "main")

    def test_main_pipeline_is_filtered_to_push_source(self) -> None:
        self.api.get_all.return_value = [{"status": "success"}]
        self.context.find_pipeline("abc", "main", pipeline_source="push")
        self.api.get_all.assert_called_once_with("pipelines", {"sha": "abc", "ref": "main", "source": "push"})

    def test_tag_pipeline_is_filtered_to_tag(self) -> None:
        self.api.get_all.return_value = [{"status": "running"}]
        self.context.find_pipeline("abc", "v5.12.0.0-1")
        self.api.get_all.assert_called_once_with("pipelines", {"sha": "abc", "ref": "v5.12.0.0-1"})
