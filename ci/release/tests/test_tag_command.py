# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from contextlib import nullcontext, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typing_extensions import override

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from commands import tag
from confirmation import ConfirmationError
from release_config import ReleaseConfig
from release_context import ReleaseContext, ReleaseWorkflowError


class TagCommandTest(unittest.TestCase):
    """Test single-tag preflight, validation, and publication."""

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
            patch.object(self.context.git, "run", return_value='{"majorVersion": 6}') as git_run,
            patch.object(self.context.git, "is_ancestor", return_value=True) as is_ancestor,
            patch.object(
                tag,
                "inspect_release_context",
                return_value=({"iid": 1}, merge_request, "merge-commit", []),
            ),
            patch.object(self.context, "find_pipeline", return_value={"status": "success"}),
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(self.context.git, "remote_tag_commit", return_value=None),
            redirect_stdout(StringIO()) as output,
        ):
            tag.run(self.context, "v6.1.0", skip_confirmation=True)
        git_run.assert_called_once_with(["show", "merge-commit:project.json"])
        is_ancestor.assert_called_once_with("merge-commit", "newer-tip")
        assert "Selected release commit to tag for v6.1.0: merge-commit" in output.getvalue()

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
                "v6.1.0",
                "branch-tip",
                skip_confirmation=True,
            )

    def test_unsuccessful_pipeline_requires_explicit_override(self) -> None:
        pipeline = {"status": "failed", "web_url": "https://gitlab.example/pipeline/42"}
        with (
            patch.object(tag, "inspect_release_context", return_value=({"iid": 1}, {}, "merge-commit", [])),
            patch.object(self.context.git, "run", return_value='{"majorVersion": 6}'),
            patch.object(self.context.git, "is_ancestor", return_value=True),
            patch.object(self.context, "find_pipeline", return_value=pipeline),
        ):
            with pytest.raises(ReleaseWorkflowError, match="--allow-unsuccessful-pipeline"):
                tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
                    self.context, "v6.1.0", "branch-tip", skip_confirmation=True
                )
            issue, commit = tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
                self.context, "v6.1.0", "branch-tip", skip_confirmation=True, allow_unsuccessful_pipeline=True
            )
        assert issue == {"iid": 1}
        assert commit == "merge-commit"

    def test_existing_remote_tag_is_rejected(self) -> None:
        with (
            patch.object(self.context.git, "local_tag_commit", return_value=None),
            patch.object(self.context.git, "remote_tag_commit", return_value="abc"),
            pytest.raises(ReleaseWorkflowError, match="already exists"),
        ):
            tag._validate_tag(self.context, "v6.1.0")  # pyright: ignore[reportPrivateUsage]

    def test_declining_final_confirmation_does_not_create_tags(self) -> None:
        with (
            patch.object(tag, "edit_tag_description", return_value="Release notes"),
            patch.object(tag, "_validate_tag"),
            patch("confirmation.sys.stdin.isatty", return_value=True),
            patch("builtins.input", return_value=""),
            patch.object(self.context.git, "create_tag") as create_tag,
            pytest.raises(ConfirmationError, match="declined"),
        ):
            tag.build_tag_plan(
                self.context,
                "v6.1.0",
                "abc123",
                {"iid": 1},
                skip_confirmation=False,
            )
        create_tag.assert_not_called()

    def test_tag_description_uses_git_editor_and_ignores_comments(self) -> None:
        def write_description(command: list[str], check: bool) -> MagicMock:
            del check
            Path(command[-1]).write_text("Maintenance fixes\n# ignored guidance\n", encoding="utf-8")
            return MagicMock(returncode=0)

        with (
            patch.object(self.context.git, "editor_command", return_value="editor"),
            patch("subprocess.run", side_effect=write_description),
        ):
            description = tag.edit_tag_description(self.context, "v6.1.0", "abc123")
        assert description == "Maintenance fixes"


@pytest.mark.parametrize("suffix", ["", ".dev2", ".rc3"])
def test_tag_command_publishes_one_tag_using_base_release_metadata(suffix: str) -> None:
    context = ReleaseContext(Path("/tmp"), ReleaseConfig(), MagicMock(), dry_run=False)
    base = "v6.1.0"
    with (
        patch.object(context.git, "resolve_remote_branch", return_value="branch-tip"),
        patch.object(tag, "_validate_release_metadata", return_value=({"iid": 1}, "release-commit")) as metadata,
        patch.object(tag, "_validate_tag"),
        patch.object(tag, "edit_tag_description", return_value="Release notes"),
        patch.object(context.git, "create_tag") as create_tag,
        patch.object(context.git, "push_tag") as push_tag,
        patch.object(tag, "_report_tag_and_pipeline"),
    ):
        tag.run(context, base + suffix, skip_confirmation=True)
    metadata.assert_called_once_with(
        context,
        base,
        "branch-tip",
        True,
        "main",
        None,
        development=suffix.startswith(".dev"),
        allow_unsuccessful_pipeline=False,
    )
    create_tag.assert_called_once_with("release-commit", base + suffix, "Release notes")
    push_tag.assert_called_once_with("origin", base + suffix)


@pytest.mark.parametrize(("merge_sha", "expected"), [("merged", "merged"), (None, "squashed")])
def test_selects_merge_result_before_squash(merge_sha: str | None, expected: str) -> None:
    context = MagicMock()
    context.find_changelog_merge_request.return_value = {
        "state": "merged",
        "merge_commit_sha": merge_sha,
        "squash_commit_sha": "squashed",
    }
    context.read_repository_file.return_value = ("## 6.1.0\n", "blob")
    assert tag.inspect_release_context(context, "v6.1.0", "main")[2] == expected


@pytest.mark.parametrize("ref", ["corrected-commit", "v6.1.0.rc2"])
def test_explicit_revision_checks_exact_commit(ref: str) -> None:
    context = MagicMock()
    context.git.run.side_effect = ["selected", '{"majorVersion": 6}']
    context.git.is_ancestor.return_value = True
    context.read_repository_file.return_value = ("## 6.1.0\n", "blob")
    context.find_pipeline.return_value = {"status": "success"}
    with patch.object(tag, "inspect_release_context", return_value=(None, {}, "merged", [])):
        _, commit = tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
            context, "v6.1.0", "tip", True, "main", ref
        )
    assert commit == "selected"
    assert context.git.is_ancestor.call_args_list[0].args == ("merged", "selected")
    assert context.git.is_ancestor.call_args_list[1].args == ("selected", "tip")
    context.find_pipeline.assert_called_once_with("selected", "main", pipeline_source="push")


@pytest.mark.parametrize(("ancestry", "match"), [([False], "contain"), ([True, False], "not reachable")])
def test_explicit_revision_rejects_unrelated_history(ancestry: list[bool], match: str) -> None:
    context = MagicMock()
    context.git.run.return_value = "selected"
    context.git.is_ancestor.side_effect = ancestry
    context.read_repository_file.return_value = ("## 6.1.0\n", "blob")
    with (
        patch.object(tag, "inspect_release_context", return_value=(None, {}, "merged", [])),
        pytest.raises(ReleaseWorkflowError, match=match),
    ):
        tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
            context, "v6.1.0", "tip", True, "main", "other"
        )
    context.git.create_tag.assert_not_called()


def test_next_candidate_uses_single_tag_plan() -> None:
    context = MagicMock()
    context.dry_run = False
    context.git.tags.return_value = ["v6.1.0.rc2"]
    with (
        patch.object(tag, "_validate_release_metadata", return_value=(None, "commit")),
        patch.object(tag, "build_tag_plan") as build_plan,
        patch.object(tag, "execute_tag_plan"),
        patch.object(tag, "_report_tag_and_pipeline"),
    ):
        tag.run(context, "v6.1.0", True, release_candidate=True)
    build_plan.assert_called_once_with(context, "v6.1.0.rc3", "commit", None, True)


@pytest.mark.parametrize("available", [True, False])
def test_pipeline_reporting_after_publication(available: bool, capsys: pytest.CaptureFixture[str]) -> None:
    context = MagicMock()
    context.config.project_web_url = "https://gitlab.example/cta"
    context.find_pipeline.return_value = {"status": "running", "web_url": "pipeline-url"} if available else None
    plan = tag.TagPlan("v6.1.0", "commit", {"iid": 1}, "Release")
    with patch.object(tag.time, "sleep"):
        tag._report_tag_and_pipeline(context, plan)  # pyright: ignore[reportPrivateUsage]
    assert context.find_pipeline.call_count == (1 if available else 6)
    assert context.add_issue_note.call_count == (2 if available else 1)
    output = capsys.readouterr().out
    assert ("pipeline-url" if available else "has not appeared yet") in output
    assert "additional release variants manually" in output


@pytest.mark.parametrize("failure", ["note", "lookup", "pipeline-note"])
def test_reporting_api_failure_does_not_fail_published_tag(failure: str, capsys: pytest.CaptureFixture[str]) -> None:
    context = MagicMock()
    context.config.project_web_url = "https://gitlab.example/cta"
    context.find_pipeline.return_value = {"status": "running", "web_url": "pipeline-url"}
    error = tag.GitLabAPIError("unavailable")
    if failure == "lookup":
        context.find_pipeline.side_effect = error
    else:
        context.add_issue_note.side_effect = [error, None] if failure == "note" else [None, error]
    plan = tag.TagPlan("v6.1.0", "commit", {"iid": 1}, "Release")
    tag._report_tag_and_pipeline(context, plan)  # pyright: ignore[reportPrivateUsage]
    output = capsys.readouterr()
    assert "Tag published successfully" in output.out
    assert "WARNING: Tag is published" in output.err


@pytest.mark.parametrize("push_status", [None, "failed", "running"])
@pytest.mark.parametrize("web_status", ["success", "failed", "running"])
def test_manual_pipeline_fallback_requires_success(push_status: str | None, web_status: str) -> None:
    context = MagicMock()
    context.git.is_ancestor.return_value = True
    context.git.run.return_value = '{"majorVersion": 6}'
    context.find_pipeline.side_effect = [
        {"status": push_status} if push_status else None,
        {"status": web_status},
    ]
    with (
        patch.object(tag, "inspect_release_context", return_value=({"iid": 1}, {}, "commit", [])),
        nullcontext() if web_status == "success" else pytest.raises(ReleaseWorkflowError, match="pipeline"),
    ):
        tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
            context, "v6.1.0", "tip", True, "maintenance"
        )
    assert [call.args for call in context.find_pipeline.call_args_list] == [
        ("commit", "maintenance"),
        ("commit", "maintenance"),
    ]
    assert [call.kwargs["pipeline_source"] for call in context.find_pipeline.call_args_list] == ["push", "web"]


@pytest.mark.parametrize(
    ("version", "next_dev"),
    [
        ("v6.1.0.dev1", False),
        ("v6.1.0", True),
    ],
)
@pytest.mark.parametrize("ref", [None, "candidate-commit"])
def test_development_needs_no_changelog(version: str, next_dev: bool, ref: str | None) -> None:
    context = MagicMock()
    context.dry_run = True
    context.config.remote = "origin"
    context.git.resolve_remote_branch.return_value = "tip"
    context.git.run.return_value = "selected"
    context.git.is_ancestor.return_value = True
    context.git.tags.return_value = []
    context.find_or_create_release_issue.return_value = None
    context.find_pipeline.return_value = {"status": "success"}
    expected_commit = "selected" if ref else "tip"
    with (
        patch.object(tag, "validate_major_version") as validate_major,
        patch.object(tag, "inspect_release_context") as inspect_metadata,
        patch.object(tag, "build_tag_plan") as build_plan,
        patch.object(tag, "confirm") as confirm,
    ):
        tag.run(context, version, False, development=next_dev, ref=ref)
    inspect_metadata.assert_not_called()
    context.read_repository_file.assert_not_called()
    context.find_or_create_release_issue.assert_called_once_with("v6.1.0", create=False)
    context.git.is_ancestor.assert_called_once_with(expected_commit, "tip")
    validate_major.assert_called_once_with(context.git, expected_commit, "v6.1.0")
    context.find_pipeline.assert_called_once_with(expected_commit, "main", pipeline_source="push")
    confirm.assert_not_called()
    expected_tag = "v6.1.0.dev1" if next_dev else version
    build_plan.assert_called_once_with(context, expected_tag, expected_commit, None, False)


def test_development_rejects_revision_outside_target_branch() -> None:
    context = MagicMock()
    context.git.run.return_value = "unrelated"
    context.git.is_ancestor.return_value = False
    with pytest.raises(ReleaseWorkflowError, match="not reachable"):
        tag._validate_release_metadata(  # pyright: ignore[reportPrivateUsage]
            context, "v6.1.0", "tip", True, ref="other", development=True
        )
    context.find_pipeline.assert_not_called()


def test_next_development_uses_development_validation() -> None:
    context = MagicMock()
    context.dry_run = True
    context.git.tags.return_value = ["v6.1.0.dev2"]
    with (
        patch.object(tag, "_validate_release_metadata", return_value=(None, "commit")) as metadata,
        patch.object(tag, "build_tag_plan") as build_plan,
    ):
        tag.run(context, "v6.1.0", True, development=True)
    assert metadata.call_args.kwargs["development"] is True
    build_plan.assert_called_once_with(context, "v6.1.0.dev3", "commit", None, True)


@pytest.mark.parametrize("version", ["v6.1.0.dev1", "v6.1.0.rc1"])
def test_development_flag_requires_base_version(version: str) -> None:
    context = MagicMock()
    with pytest.raises(tag.VersionError, match="unsuffixed"):
        tag.run(context, version, True, development=True)
    context.git.resolve_remote_branch.assert_not_called()
