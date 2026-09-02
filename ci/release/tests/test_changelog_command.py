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

from commands import changelog
from cta_version import CTAVersion
from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError


class ChangelogCommandTest(unittest.TestCase):
    """Test changelog generation and release-resource metadata."""

    @override
    def setUp(self) -> None:
        self.api = MagicMock()
        self.context = ReleaseContext(Path("/tmp"), ReleaseConfig(), self.api, dry_run=False)

    def test_preview_includes_commits_without_valid_trailers(self) -> None:
        self.api.get.return_value = {"notes": "## 5.12.0.0-1\n\n### Bug Fixes\n\n- Fixed"}
        self.api.get_all.return_value = [
            {"title": "Missing trailer", "web_url": "https://gitlab.example/commit/abc", "trailers": {}},
            {
                "title": "Valid fix",
                "web_url": "https://gitlab.example/commit/def",
                "trailers": {"Changelog": "fix"},
            },
            {
                "title": "Invalid category",
                "web_url": "https://gitlab.example/commit/ghi",
                "trailers": {"Changelog": "typo"},
            },
        ]
        notes = changelog.generate_changelog_notes(self.context, "old", "new", "5.12.0.0-1")
        assert "[Missing trailer](https://gitlab.example/commit/abc)" in notes
        assert "[Invalid category](https://gitlab.example/commit/ghi)" in notes
        assert "Valid fix" not in notes
        assert "Remove these entries or move them" in notes

    def test_issue_note_contains_clickable_release_context(self) -> None:
        note = changelog.changelog_issue_note(
            self.context,
            "v5.12.0.0-1",
            "abcdef1234567890",
            "v5.12.0.0-1-changelog-update",
            "https://gitlab.example/branch",
            {"iid": 42, "web_url": "https://gitlab.example/mr/42"},
        )
        assert "[`abcdef123456`](https://gitlab.cern.ch/cta/CTA/-/commit/abcdef1234567890)" in note
        assert "[`v5.12.0.0-1-changelog-update`](https://gitlab.example/branch)" in note
        assert "[merge request !42](https://gitlab.example/mr/42)" in note
        assert "will be tagged as **v5.12.0.0-1**" in note
        assert note.endswith("Awaiting MR approval and merge.")

    def test_release_merge_request_metadata(self) -> None:
        description = changelog.merge_request_description("v5.12.0.0-1", 42)
        assert description.startswith("### Description\n\n")
        assert "Updates the changelog in preparation for release v5.12.0.0-1." in description
        assert "See #42" in description
        assert "\n\n### Checklist\n\n" in description
        assert changelog.MERGE_REQUEST_LABELS == ("type::release", "priority::high")

    def test_release_merge_request_assigns_authenticated_user(self) -> None:
        assert changelog.authenticated_user_id({"id": 42, "username": "release-manager"}) == 42

    def test_confirms_changelog_publication_after_editing(self) -> None:
        with patch("builtins.input", return_value="yes") as user_input:
            changelog.confirm_changelog_publication()
        user_input.assert_called_once_with("Continue and publish the edited changelog? [y/N] ")

    def test_declining_changelog_publication_aborts(self) -> None:
        with (
            patch("builtins.input", return_value=""),
            pytest.raises(ReleaseWorkflowError, match="publication declined"),
        ):
            changelog.confirm_changelog_publication()

    def test_declining_existing_changelog_resources_does_not_create_issue(self) -> None:
        branch = {"name": "v5.12.0.0-1-changelog-update", "commit": {"id": "abc123"}}
        merge_request = {"state": "opened", "web_url": "https://gitlab.example/mr/42"}
        with (
            patch.object(changelog, "edit_changelog_notes", return_value="## 5.12.0.0-1\n"),
            patch.object(changelog, "confirm_changelog_publication"),
            patch.object(self.api, "get_all", return_value=[branch]),
            patch.object(self.context, "find_or_create_release_issue") as find_issue,
            patch("builtins.input", return_value=""),
            pytest.raises(ReleaseWorkflowError, match="resource reuse declined"),
        ):
            changelog._publish_changelog(  # pyright: ignore[reportPrivateUsage]
                self.context,
                CTAVersion.parse("v5.12.0.0-1"),
                "abc123",
                "## 5.12.0.0-1\n",
                7,
                merge_request,
            )
        find_issue.assert_not_called()

    def test_rejects_authenticated_user_without_numeric_id(self) -> None:
        self.api.authenticate.return_value = {"username": "release-manager"}
        with (
            patch.object(self.context.git, "validate_repository", return_value="abc123"),
            pytest.raises(ReleaseWorkflowError, match="no valid numeric ID"),
        ):
            changelog.run(self.context, "v5.12.0.0-1")

    def test_newer_tags_do_not_block_an_available_maintenance_tag(self) -> None:
        version = CTAVersion.parse("v5.11.18.1-1")
        with (
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(self.context.git, "remote_tag_commit", return_value=None),
        ):
            changelog._validate_tag_is_available(self.context, version)  # pyright: ignore[reportPrivateUsage]

    def test_exact_existing_tag_is_rejected(self) -> None:
        version = CTAVersion.parse("v5.11.18.1-1")
        with (
            patch.object(self.context.git, "local_tag_commit", return_value="abc123"),
            pytest.raises(ReleaseWorkflowError, match="already exists"),
        ):
            changelog._validate_tag_is_available(self.context, version)  # pyright: ignore[reportPrivateUsage]
