# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

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

    def test_discovers_single_unfinished_release(self) -> None:
        self.api.get_page.return_value = [
            {"state": "merged", "target_branch": "main", "source_branch": "v5.12.0.0-1-changelog-update"}
        ]
        with patch.object(self.context.git, "remote_tag_names", return_value=set()):
            assert self.context.discover_unfinished_release_version() == "v5.12.0.0-1"
        self.api.get_page.assert_called_once_with(
            "merge_requests",
            {
                "scope": "all",
                "target_branch": "main",
                "state": "merged",
                "labels": "type::release",
                "order_by": "updated_at",
                "sort": "desc",
            },
            per_page=20,
        )

    def test_discovery_rejects_ambiguity(self) -> None:
        self.api.get_page.return_value = [
            {"state": "merged", "source_branch": f"{version}-changelog-update"}
            for version in ("v5.12.0.0-1", "v5.12.1.0-1")
        ]
        with (
            patch.object(self.context.git, "remote_tag_names", return_value=set()),
            pytest.raises(ReleaseWorkflowError, match="Multiple unfinished releases"),
        ):
            self.context.discover_unfinished_release_version()

    def test_discovery_ignores_unmerged_and_tagged_releases(self) -> None:
        self.api.get_page.return_value = [
            {"state": "opened", "source_branch": "v5.12.0.0-1-changelog-update"},
            {"state": "merged", "source_branch": "v5.12.1.0-1-changelog-update"},
        ]
        with (
            patch.object(self.context.git, "remote_tag_names", return_value={"v5.12.1.0-1"}),
            pytest.raises(ReleaseWorkflowError, match="No merged"),
        ):
            self.context.discover_unfinished_release_version()

    def test_main_pipeline_is_filtered_to_push_source(self) -> None:
        self.api.get_all.return_value = [{"status": "success"}]
        self.context.find_pipeline("abc", "main", pipeline_source="push")
        self.api.get_all.assert_called_once_with("pipelines", {"sha": "abc", "ref": "main", "source": "push"})

    def test_tag_pipeline_is_filtered_to_tag(self) -> None:
        self.api.get_all.return_value = [{"status": "running"}]
        self.context.find_pipeline("abc", "v5.12.0.0-1")
        self.api.get_all.assert_called_once_with("pipelines", {"sha": "abc", "ref": "v5.12.0.0-1"})
