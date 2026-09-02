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
from cta_version import BuildVariant
from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError


class TagCommandTest(unittest.TestCase):
    """Test tag-family selection, validation, and publication."""

    @override
    def setUp(self) -> None:
        self.api = MagicMock()
        self.context = ReleaseContext(Path("/tmp"), ReleaseConfig(), self.api, dry_run=False)

    def test_prints_commit_and_publishes_all_variants_with_yes(self) -> None:
        self.context.dry_run = True
        self.context.git.dry_run = True
        with (
            patch.object(self.context.git, "resolve_tag_target", return_value="abc123"),
            patch.object(self.context, "find_pipeline", return_value={"status": "success"}) as find_pipeline,
            patch.object(tag, "inspect_release_context", return_value=({"iid": 1}, [])),
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(self.context.git, "remote_tag_commit", return_value=None),
            patch.object(self.context.git, "create_tags") as create_tags,
            patch.object(self.context.git, "push_tags") as push_tags,
            patch.object(self.context, "add_issue_note"),
            redirect_stdout(StringIO()) as output,
        ):
            tag.run(self.context, "v5.12.0.0-1", skip_confirmation=True, requested_target_ref=None)
        assert "Commit to tag for v5.12.0.0-1: abc123" in output.getvalue()
        find_pipeline.assert_called_once_with("abc123", pipeline_source="push")
        assert list(create_tags.call_args.args[1]) == [
            "v5.12.0.0-1",
            "v5.12.0.0-1.pgsched",
            "v5.12.0.0-1.pgcat",
            "v5.12.0.0-1.pgall",
        ]
        push_tags.assert_called_once_with("origin", list(create_tags.call_args.args[1]))

    def test_explicit_suffixes_create_only_canonical_variants(self) -> None:
        self.context.dry_run = True
        self.context.git.dry_run = True
        with (
            patch.object(self.context.git, "resolve_tag_target", return_value="abc123"),
            patch.object(self.context, "find_pipeline", return_value={"status": "success"}),
            patch.object(tag, "inspect_release_context", return_value=({"iid": 1}, [])),
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(self.context.git, "remote_tag_commit", return_value=None),
            patch.object(self.context.git, "create_tags") as create_tags,
            patch.object(self.context.git, "push_tags") as push_tags,
            patch.object(self.context, "add_issue_note"),
        ):
            tag.run(
                self.context,
                "v5.12.0.0-1",
                skip_confirmation=True,
                requested_target_ref=None,
                requested_suffixes=["pgall", "pgsched", "pgall"],
            )
        assert list(create_tags.call_args.args[1]) == ["v5.12.0.0-1.pgsched", "v5.12.0.0-1.pgall"]
        push_tags.assert_called_once_with("origin", ["v5.12.0.0-1.pgsched", "v5.12.0.0-1.pgall"])

    def test_variant_prompt_controls_default_family(self) -> None:
        for answer, expected_tags in (
            ("n", ["v5.12.0.0-1"]),
            (
                "y",
                [
                    "v5.12.0.0-1",
                    "v5.12.0.0-1.pgsched",
                    "v5.12.0.0-1.pgcat",
                    "v5.12.0.0-1.pgall",
                ],
            ),
        ):
            with (
                self.subTest(answer=answer),
                patch("builtins.input", return_value=answer),
                patch.object(self.context.git, "resolve_tag_target", return_value="abc123"),
                patch.object(
                    self.context,
                    "find_pipeline",
                    return_value={"status": "success", "web_url": "https://gitlab.example/pipeline"},
                ),
                patch.object(tag, "inspect_release_context", return_value=(None, [])),
                patch.object(self.context.git, "local_tag_commit", return_value="abc123"),
                patch.object(self.context.git, "remote_tag_commit", return_value="abc123") as remote_tag,
                patch.object(self.context, "add_issue_note"),
                redirect_stdout(StringIO()),
            ):
                tag.run(self.context, "v5.12.0.0-1", skip_confirmation=False, requested_target_ref=None)
            assert [tag_call.args[1] for tag_call in remote_tag.call_args_list] == expected_tags

    def test_release_candidate_uses_recoverable_family_number(self) -> None:
        self.context.dry_run = True
        self.context.git.dry_run = True
        with (
            patch.object(self.context.git, "resolve_tag_target", return_value="abc123"),
            patch.object(self.context, "find_pipeline", return_value={"status": "success"}),
            patch.object(tag, "inspect_release_context", return_value=({"iid": 1}, [])),
            patch.object(
                self.context.git,
                "tags",
                return_value=["v5.12.0.0-1.rc2", "v5.12.0.0-1.rc2.pgsched"],
            ),
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(self.context.git, "remote_tag_commit", return_value=None),
            patch.object(self.context.git, "create_tags") as create_tags,
            patch.object(self.context.git, "push_tags"),
            patch.object(self.context, "add_issue_note"),
        ):
            tag.run(
                self.context,
                "v5.12.0.0-1",
                skip_confirmation=True,
                requested_target_ref=None,
                release_candidate=True,
            )
        assert list(create_tags.call_args.args[1]) == [
            "v5.12.0.0-1.rc2",
            "v5.12.0.0-1.rc2.pgsched",
            "v5.12.0.0-1.rc2.pgcat",
            "v5.12.0.0-1.rc2.pgall",
        ]

    def test_variant_descriptions_extend_shared_text(self) -> None:
        assert tag.build_tag_description("Shared", None) == "Shared"
        assert tag.build_tag_description("Shared", BuildVariant.PGCAT) == (
            "Shared\n\nThis is a PostgreSQL catalogue release without Oracle support; "
            "its Docker images are safe for publication."
        )

    def test_tag_description_uses_git_editor_and_ignores_comments(self) -> None:
        def write_description(command: list[str], check: bool) -> MagicMock:
            """Write a simulated editor result to its temporary file."""
            del check
            Path(command[-1]).write_text("Maintenance fixes\n# ignored guidance\n", encoding="utf-8")
            return MagicMock(returncode=0)

        with (
            patch.object(self.context.git, "editor_command", return_value="editor"),
            patch("subprocess.run", side_effect=write_description),
        ):
            description = tag.edit_tag_description(self.context, "v5.12.0.0-1", "abc123")
        assert description == "Maintenance fixes"

    def test_explicit_version_can_override_missing_release_context(self) -> None:
        self.context.dry_run = True
        self.context.git.dry_run = True
        with (
            patch.object(self.context.git, "resolve_tag_target", return_value="abc123"),
            patch.object(tag, "inspect_release_context", return_value=(None, ["Release issue was not found"])),
            patch.object(self.context, "find_pipeline", return_value={"status": "success"}),
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(self.context.git, "remote_tag_commit", return_value=None),
            patch.object(self.context, "add_issue_note"),
        ):
            tag.run(self.context, "v5.12.0.0-1", skip_confirmation=True, requested_target_ref=None)

    def test_unsuccessful_pipeline_can_be_confirmed(self) -> None:
        pipeline = {"status": "failed", "web_url": "https://gitlab.example/pipeline/42"}
        with (
            patch.object(tag, "inspect_release_context", return_value=({"iid": 1}, [])),
            patch.object(self.context, "find_pipeline", return_value=pipeline),
            patch("builtins.input", return_value="yes") as user_input,
        ):
            release_issue, confirmed = tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
                self.context,
                "v5.12.0.0-1",
                "abc123",
                version_was_explicit=True,
                skip_confirmation=False,
            )
        assert release_issue == {"iid": 1}
        assert confirmed
        user_input.assert_called_once_with("Continue and create tag v5.12.0.0-1 without a successful pipeline? [y/N] ")

    def test_missing_pipeline_declines_by_default(self) -> None:
        with (
            patch.object(tag, "inspect_release_context", return_value=({"iid": 1}, [])),
            patch.object(self.context, "find_pipeline", return_value=None),
            patch("builtins.input", return_value=""),
            pytest.raises(ReleaseWorkflowError, match="Tag creation declined"),
        ):
            tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
                self.context,
                "v5.12.0.0-1",
                "abc123",
                version_was_explicit=True,
                skip_confirmation=False,
            )

    def test_inferred_version_rejects_missing_release_context(self) -> None:
        with (
            patch.object(self.context, "discover_unfinished_release_version", return_value="v5.12.0.0-1"),
            patch.object(self.context.git, "resolve_tag_target", return_value="abc123"),
            patch.object(tag, "inspect_release_context", return_value=(None, ["Changelog entry was not found"])),
            pytest.raises(ReleaseWorkflowError, match="explicit version"),
        ):
            tag.run(self.context, None, skip_confirmation=True, requested_target_ref=None)
