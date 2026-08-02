# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from typing_extensions import override

RELEASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RELEASE_DIR))

from cta_version import (  # noqa: E402
    CTAVersion,
    VersionError,
    parsed_versions,
    previous_release,
)
from ci.release.git_repo import Git  # noqa: E402
from gitlab_api import GitLabAPI, GitLabAPIError  # noqa: E402
from release_cli import MERGE_REQUEST_LABELS, ReleaseService, merge_request_description  # noqa: E402
from release_cli import ReleaseWorkflowError  # noqa: E402
from release_config import ReleaseConfig  # noqa: E402


class VersionTest(unittest.TestCase):
    def test_parses_canonical_versions(self) -> None:
        version = CTAVersion.parse("v5.10.11.0-1.rc1")
        assert version.core == (5, 10, 11, 0, 1)
        assert version.suffix == "rc1"

    def test_rejects_invalid_versions(self) -> None:
        for value in ("5.10.11.0-1", "v4.10.11.0-1", "v5.1.2-3", "v5.1.2.3-4.rc-1"):
            with self.subTest(value=value), pytest.raises(VersionError):
                CTAVersion.parse(value)

    def test_suffix_does_not_change_numeric_core(self) -> None:
        assert CTAVersion.parse("v5.10.11.0-1").core == CTAVersion.parse("v5.10.11.0-1.rc1").core

    def test_previous_release_uses_numeric_order(self) -> None:
        tags = ["not-a-release", "v5.9.99.0-1", "v5.10.2.0-1", "v5.10.11.0-1.rc1"]
        result = previous_release(CTAVersion.parse("v5.10.12.0-1"), tags)
        assert result.text == "v5.10.11.0-1.rc1"
        assert len(parsed_versions(tags)) == 3


class GitTest(unittest.TestCase):
    def test_dry_run_does_not_execute_mutation(self) -> None:
        git = Git(Path("/tmp"), dry_run=True)
        with patch("subprocess.run") as run:
            assert git.run(["tag", "v5.1.0.0-1"], mutate=True) == ""
        run.assert_not_called()

    def test_create_tag_uses_annotated_tag_and_explicit_refspec(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(git, "run") as run:
            git.create_tag("origin", "v5.1.0.0-1", "abc", "Maintenance bug fixes")
        assert [
            call(["tag", "-a", "v5.1.0.0-1", "abc", "-m", "Maintenance bug fixes"], mutate=True),
            call(["push", "origin", "refs/tags/v5.1.0.0-1:refs/tags/v5.1.0.0-1"], mutate=True),
        ] == run.call_args_list

    def test_editor_command_uses_git_editor_resolution(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(git, "run", return_value="vim") as run:
            assert git.editor_command() == "vim"
        run.assert_called_once_with(["var", "GIT_EDITOR"])

    def test_allow_unclean_skips_worktree_and_current_branch_checks(self) -> None:
        git = Git(Path("/tmp"), allow_unclean=True)
        with patch.object(
            git,
            "run",
            side_effect=["/tmp", "abc", "abc"],
        ) as run:
            assert git.validate_repository("main", "origin", fetch=False) == "abc"
        assert [
            call(["rev-parse", "--show-toplevel"]),
            call(["rev-parse", "main"]),
            call(["rev-parse", "origin/main"]),
        ] == run.call_args_list

    def test_default_tag_target_is_latest_remote_main(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(
            git,
            "run",
            side_effect=["/tmp", "", "abc123"],
        ) as run:
            assert git.resolve_tag_target("origin", "main", fetch=False) == ("origin/main", "abc123")
        assert call(["rev-parse", "--verify", "origin/main^{commit}"]) == run.call_args_list[-1]

    def test_explicit_tag_target_accepts_any_git_revision(self) -> None:
        git = Git(Path("/tmp"), allow_unclean=True)
        with patch.object(
            git,
            "run",
            side_effect=["/tmp", "def456"],
        ) as run:
            assert git.resolve_tag_target("origin", "main", ref="maintenance", fetch=False) == ("maintenance", "def456")
        run.assert_called_with(["rev-parse", "--verify", "maintenance^{commit}"])


class GitLabAPITest(unittest.TestCase):
    def test_project_id_is_url_encoded(self) -> None:
        api = GitLabAPI("https://gitlab.example", "group/project", "secret")
        assert api.project_url == "https://gitlab.example/api/v4/projects/group%2Fproject"

    def test_paginates(self) -> None:
        api = GitLabAPI("https://gitlab.example", "1", "secret")
        with patch.object(
            api,
            "_request",
            side_effect=[([{"id": 1}], {"x-next-page": "2"}), ([{"id": 2}], {})],
        ) as request:
            assert api.get_all("issues", {"scope": "all"}) == [{"id": 1}, {"id": 2}]
        assert request.call_args_list[0].kwargs["params"]["page"] == 1
        assert request.call_args_list[1].kwargs["params"]["page"] == 2

    def test_rejects_non_list_paginated_response(self) -> None:
        api = GitLabAPI("https://gitlab.example", "1", "secret")
        with patch.object(api, "_request", return_value=({"message": "bad"}, {})):
            with pytest.raises(GitLabAPIError):
                api.get_all("issues")


class DiscoveryTest(unittest.TestCase):
    @override
    def setUp(self) -> None:
        self.api = MagicMock()
        self.service = ReleaseService(Path("/tmp"), ReleaseConfig(), self.api, dry_run=False)

    def test_discovers_single_unfinished_release(self) -> None:
        self.api.get_all.return_value = [
            {
                "state": "merged",
                "target_branch": "main",
                "source_branch": "v5.12.0.0-1-changelog-update",
            }
        ]
        with patch.object(self.service.git, "remote_tag_commit", return_value=None):
            assert self.service.discover_unfinished_release_version() == "v5.12.0.0-1"

    def test_discovery_rejects_ambiguity(self) -> None:
        self.api.get_all.return_value = [
            {"state": "merged", "source_branch": f"{version}-changelog-update"}
            for version in ("v5.12.0.0-1", "v5.12.1.0-1")
        ]
        with patch.object(self.service.git, "remote_tag_commit", return_value=None):
            with pytest.raises(ReleaseWorkflowError, match="Multiple unfinished releases"):
                self.service.discover_unfinished_release_version()

    def test_discovery_ignores_unmerged_and_tagged_releases(self) -> None:
        self.api.get_all.return_value = [
            {"state": "opened", "source_branch": "v5.12.0.0-1-changelog-update"},
            {"state": "merged", "source_branch": "v5.12.1.0-1-changelog-update"},
        ]
        with patch.object(self.service.git, "remote_tag_commit", return_value="abc"):
            with pytest.raises(ReleaseWorkflowError, match="No merged"):
                self.service.discover_unfinished_release_version()

    def test_main_pipeline_is_filtered_to_push_source(self) -> None:
        self.api.get_all.return_value = [{"status": "success"}]
        self.service.find_pipeline("abc", "main", source="push")
        self.api.get_all.assert_called_once_with("pipelines", {"sha": "abc", "ref": "main", "source": "push"})

    def test_tag_pipeline_is_filtered_to_tag(self) -> None:
        self.api.get_all.return_value = [{"status": "running"}]
        self.service.find_pipeline("abc", "v5.12.0.0-1")
        self.api.get_all.assert_called_once_with("pipelines", {"sha": "abc", "ref": "v5.12.0.0-1"})

    def test_tag_prints_commit_selected_for_tagging(self) -> None:
        self.service.dry_run = True
        self.service.git.dry_run = True
        with (
            patch.object(
                self.service.git,
                "resolve_tag_target",
                return_value=("origin/main", "abc123"),
            ),
            patch.object(
                self.service,
                "find_pipeline",
                return_value={"status": "success"},
            ),
            patch.object(
                self.service,
                "inspect_tag_release_context",
                return_value=({"iid": 1}, []),
            ),
            patch.object(self.service.git, "local_tag_commit", return_value=None),
            patch.object(self.service.git, "remote_tag_commit", return_value=None),
            patch.object(self.service, "add_issue_note"),
            redirect_stdout(StringIO()) as output,
        ):
            self.service.tag_release("v5.12.0.0-1", yes=True, target_ref=None)
        assert "Commit to tag for v5.12.0.0-1: abc123" in output.getvalue()
        assert "Tag page: https://gitlab.cern.ch/cta/CTA/-/tags/v5.12.0.0-1" in output.getvalue()

    def test_tag_description_uses_git_editor_and_ignores_comments(self) -> None:
        def write_description(command: list[str], check: bool) -> MagicMock:
            del check
            Path(command[-1]).write_text("Maintenance fixes\n# ignored guidance\n", encoding="utf-8")
            return MagicMock(returncode=0)

        with (
            patch.object(self.service.git, "editor_command", return_value="editor"),
            patch("subprocess.run", side_effect=write_description),
        ):
            description = self.service.edit_tag_description("v5.12.0.0-1", "abc123")
        assert description == "Maintenance fixes"

    def test_newer_tags_do_not_block_an_available_maintenance_tag(self) -> None:
        version = CTAVersion.parse("v5.11.18.1-1")
        with (
            patch.object(self.service.git, "local_tag_commit", return_value=None),
            patch.object(self.service.git, "remote_tag_commit", return_value=None),
            patch.object(
                self.service.git,
                "tags",
                return_value=["v5.11.19.0-2", "v5.999.3.0-1.test"],
            ),
        ):
            self.service._validate_tag_is_available(version)  # pyright: ignore[reportPrivateUsage]

    def test_exact_existing_tag_is_rejected(self) -> None:
        version = CTAVersion.parse("v5.11.18.1-1")
        with patch.object(self.service.git, "local_tag_commit", return_value="abc123"):
            with pytest.raises(ReleaseWorkflowError, match="already exists"):
                self.service._validate_tag_is_available(version)  # pyright: ignore[reportPrivateUsage]

    def test_explicit_version_can_override_missing_release_context(self) -> None:
        self.service.dry_run = True
        self.service.git.dry_run = True
        with (
            patch.object(
                self.service.git,
                "resolve_tag_target",
                return_value=("origin/main", "abc123"),
            ),
            patch.object(
                self.service,
                "inspect_tag_release_context",
                return_value=(None, ["Release issue was not found"]),
            ),
            patch.object(
                self.service,
                "find_pipeline",
                return_value={"status": "success"},
            ),
            patch.object(self.service.git, "local_tag_commit", return_value=None),
            patch.object(self.service.git, "remote_tag_commit", return_value=None),
            patch.object(self.service, "add_issue_note"),
        ):
            self.service.tag_release("v5.12.0.0-1", yes=True, target_ref=None)

    def test_inferred_version_rejects_missing_release_context(self) -> None:
        with (
            patch.object(
                self.service,
                "discover_unfinished_release_version",
                return_value="v5.12.0.0-1",
            ),
            patch.object(
                self.service.git,
                "resolve_tag_target",
                return_value=("origin/main", "abc123"),
            ),
            patch.object(
                self.service,
                "inspect_tag_release_context",
                return_value=(None, ["Changelog entry was not found"]),
            ),
            pytest.raises(ReleaseWorkflowError, match="explicit version"),
        ):
            self.service.tag_release(None, yes=True, target_ref=None)

    def test_changelog_preview_includes_commits_without_valid_trailers(self) -> None:
        self.api.get.return_value = {"notes": "## 5.12.0.0-1\n\n### Bug Fixes\n\n- Fixed"}
        self.api.get_all.return_value = [
            {
                "title": "Missing trailer",
                "web_url": "https://gitlab.example/commit/abc",
                "trailers": {},
            },
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
        notes = self.service.generate_changelog_notes("old", "new", "5.12.0.0-1")
        assert "[Missing trailer](https://gitlab.example/commit/abc)" in notes
        assert "[Invalid category](https://gitlab.example/commit/ghi)" in notes
        assert "Valid fix" not in notes
        assert "Remove these entries or move them" in notes

    def test_changelog_issue_note_contains_clickable_release_context(self) -> None:
        note = self.service.changelog_issue_note(
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
        description = merge_request_description("v5.12.0.0-1", 42)
        assert description.startswith("### Description\n\n")
        assert "Updates the changelog in preparation for release v5.12.0.0-1." in description
        assert "Closes #42" in description
        assert "\n\n### Checklist\n\n" in description
        assert MERGE_REQUEST_LABELS == ("type::release", "priority::high")


if __name__ == "__main__":
    unittest.main()
