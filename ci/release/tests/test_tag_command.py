# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typing_extensions import override

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from commands import tag
from confirmation import ConfirmationError
from cta_version import BUILD_VARIANTS, CTAVersion, BuildVariant
from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError


class TagCommandTest(unittest.TestCase):
    """Test tag-family preflight, validation, and publication."""

    @override
    def setUp(self) -> None:
        self.api = MagicMock()
        self.context = ReleaseContext(Path("/tmp"), ReleaseConfig(), self.api, dry_run=False)

    def test_tags_merged_changelog_commit_when_branch_has_advanced(self) -> None:
        self.context.dry_run = True
        self.context.git.dry_run = True
        merge_request = {"state": "merged", "web_url": "https://gitlab.example/mr/1"}
        with (
            patch.object(self.context.git, "validate_target_branch"),
            patch.object(self.context.git, "resolve_remote_branch", return_value="newer-tip"),
            patch.object(self.context.git, "is_ancestor", return_value=True) as is_ancestor,
            patch.object(
                tag,
                "inspect_release_context",
                return_value=({"iid": 1}, merge_request, "merge-commit", []),
            ),
            patch.object(self.context, "find_pipeline", return_value={"status": "success"}),
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(self.context.git, "remote_tag_commits", return_value={}),
            redirect_stdout(StringIO()) as output,
        ):
            tag.run(self.context, "v5.12.0.0-1", skip_confirmation=True)
        is_ancestor.assert_called_once_with("merge-commit", "newer-tip")
        assert "Merged changelog commit to tag for v5.12.0.0-1: merge-commit" in output.getvalue()

    def test_release_commit_must_be_reachable_from_target_branch(self) -> None:
        with (
            patch.object(
                tag,
                "inspect_release_context",
                return_value=(None, {"state": "merged"}, "merge-commit", []),
            ),
            patch.object(self.context.git, "is_ancestor", return_value=False),
            pytest.raises(ReleaseWorkflowError, match="not reachable"),
        ):
            tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
                self.context,
                "v5.12.0.0-1",
                "branch-tip",
                skip_confirmation=True,
            )

    def test_unsuccessful_pipeline_has_separate_confirmation(self) -> None:
        pipeline = {"status": "failed", "web_url": "https://gitlab.example/pipeline/42"}
        with (
            patch.object(
                tag,
                "inspect_release_context",
                return_value=({"iid": 1}, {"state": "merged"}, "merge-commit", []),
            ),
            patch.object(self.context.git, "is_ancestor", return_value=True),
            patch.object(self.context, "find_pipeline", return_value=pipeline),
            patch("confirmation.sys.stdin.isatty", return_value=True),
            patch("builtins.input", return_value="yes") as user_input,
        ):
            issue, commit = tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
                self.context,
                "v5.12.0.0-1",
                "branch-tip",
                skip_confirmation=False,
            )
        assert issue == {"iid": 1}
        assert commit == "merge-commit"
        user_input.assert_called_once()

    def test_any_existing_tag_rejects_complete_family(self) -> None:
        with (
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(
                self.context.git,
                "remote_tag_commits",
                return_value={"v5.12.0.0-1.pgall": "abc123"},
            ) as remote_tags,
            pytest.raises(ReleaseWorkflowError, match="family already exists"),
        ):
            tag._validate_selected_tags(  # pyright: ignore[reportPrivateUsage]
                self.context,
                ["v5.12.0.0-1", "v5.12.0.0-1.pgall"],
                "abc123",
            )
        remote_tags.assert_called_once()

    def test_next_release_candidate_never_completes_partial_family(self) -> None:
        version = CTAVersion.parse("v5.12.0.0-1", require_base=True)
        with patch.object(
            self.context.git,
            "tags",
            return_value=["v5.12.0.0-1.rc2", "v5.12.0.0-1.rc2.pgsched"],
        ):
            selected = tag._select_tag_versions(  # pyright: ignore[reportPrivateUsage]
                self.context,
                version,
                BUILD_VARIANTS,
                variants_explicitly_selected=False,
                release_candidate=True,
            )
        assert selected[0].text == "v5.12.0.0-1.rc3"

    def test_declining_final_confirmation_does_not_create_tags(self) -> None:
        with (
            patch.object(tag, "edit_tag_description", return_value="Release notes"),
            patch.object(tag, "_validate_selected_tags"),
            patch("confirmation.sys.stdin.isatty", return_value=True),
            patch("builtins.input", return_value=""),
            patch.object(self.context.git, "create_tags") as create_tags,
            pytest.raises(ConfirmationError, match="declined"),
        ):
            tag.build_tag_plan(
                self.context,
                "v5.12.0.0-1",
                "main",
                "abc123",
                {"iid": 1},
                [CTAVersion.parse("v5.12.0.0-1")],
                skip_confirmation=False,
            )
        create_tags.assert_not_called()

    def test_variant_descriptions_extend_shared_text(self) -> None:
        assert tag.build_tag_description("Shared", None) == "Shared"
        assert tag.build_tag_description("Shared", BuildVariant.PGCAT) == (
            "Shared\n\nThis is a PostgreSQL catalogue release without Oracle support; "
            "its Docker images are safe for publication."
        )

    def test_tag_description_uses_git_editor_and_ignores_comments(self) -> None:
        def write_description(command: list[str], check: bool) -> MagicMock:
            del check
            Path(command[-1]).write_text("Maintenance fixes\n# ignored guidance\n", encoding="utf-8")
            return MagicMock(returncode=0)

        with (
            patch.object(self.context.git, "editor_command", return_value="editor"),
            patch("subprocess.run", side_effect=write_description),
        ):
            description = tag.edit_tag_description(self.context, "v5.12.0.0-1", "abc123")
        assert description == "Maintenance fixes"
