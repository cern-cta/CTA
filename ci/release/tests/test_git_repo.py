# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import call, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from git_repo import Git, GitError


class GitTest(unittest.TestCase):
    """Test release-specific Git validation and mutation commands."""

    def test_dry_run_does_not_execute_mutation(self) -> None:
        git = Git(Path("/tmp"), dry_run=True)
        with patch("subprocess.run") as run:
            assert git.run(["tag", "v6.1.0.0-1"], mutate=True) == ""
        run.assert_not_called()

    def test_create_tag_delegates_to_multi_tag_creation(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(git, "create_tags") as create_tags:
            git.create_tag("abc", "v6.1.0.0-1", "Maintenance bug fixes")
        create_tags.assert_called_once_with("abc", {"v6.1.0.0-1": "Maintenance bug fixes"})

    def test_editor_command_uses_git_editor_resolution(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(git, "run", return_value="vim") as run:
            assert git.editor_command() == "vim"
        run.assert_called_once_with(["var", "GIT_EDITOR"])

    def test_confirms_dirty_non_release_checkout(self) -> None:
        git = Git(Path("/tmp"))
        with (
            patch.object(git, "run", side_effect=["", "/tmp", "topic", "dirty", "abc", "abc"]) as run,
            patch("builtins.input", return_value="yes") as user_input,
            patch("confirmation.sys.stdin.isatty", return_value=True),
        ):
            assert git.validate_repository("main", "origin", fetch=False) == "abc"
        assert run.call_args_list == [
            call(["check-ref-format", "--branch", "main"]),
            call(["rev-parse", "--show-toplevel"]),
            call(["branch", "--show-current"]),
            call(["status", "--porcelain"]),
            call(["rev-parse", "main"]),
            call(["rev-parse", "origin/main"]),
        ]
        user_input.assert_called_once_with("Continue with this checkout? [y/N] ")

    def test_fast_forwards_current_release_branch(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(git, "run", side_effect=["", "/tmp", "main", "", "old", "new", "", ""]) as run:
            assert git.validate_repository("main", "origin", fetch=False) == "new"
        assert run.call_args_list[-1] == call(["merge", "--ff-only", "origin/main"], mutate=True)

    def test_confirmed_checkout_updates_release_branch_without_switching(self) -> None:
        git = Git(Path("/tmp"))
        with (
            patch.object(git, "run", side_effect=["", "/tmp", "topic", "", "old", "new", "", ""]) as run,
            patch("builtins.input", return_value="yes"),
            patch("confirmation.sys.stdin.isatty", return_value=True),
        ):
            assert git.validate_repository("main", "origin", fetch=False) == "new"
        assert run.call_args_list[-1] == call(
            ["branch", "--force", "main", "origin/main"],
            mutate=True,
        )

    def test_does_not_overwrite_local_release_branch_commits(self) -> None:
        git = Git(Path("/tmp"))
        with (
            patch.object(
                git,
                "run",
                side_effect=["", "/tmp", "topic", "", "local", "remote", GitError("not an ancestor")],
            ),
            patch("builtins.input", return_value="yes"),
            patch("confirmation.sys.stdin.isatty", return_value=True),
            pytest.raises(GitError, match="Cannot fast-forward main"),
        ):
            git.validate_repository("main", "origin", fetch=False)

    def test_fetch_refreshes_remote_branch_and_tags(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(git, "run", side_effect=["", "/tmp", "main", "", "", "new", "new"]) as run:
            assert git.validate_repository("main", "origin") == "new"
        assert call(["fetch", "--force", "--tags", "origin", "main"], mutate=True) in run.call_args_list

    def test_resolves_latest_remote_branch(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(git, "run", side_effect=["", "/tmp", "", "abc123"]) as run:
            assert git.resolve_remote_branch("origin", "main", fetch=False) == "abc123"
        assert run.call_args_list[-1] == call(["rev-parse", "--verify", "origin/main^{commit}"])

    def test_rejects_invalid_target_branch(self) -> None:
        git = Git(Path("/tmp"))
        with (
            patch.object(git, "run", side_effect=GitError("invalid")),
            pytest.raises(GitError, match="Invalid target branch"),
        ):
            git.resolve_remote_branch("origin", "--invalid", fetch=False)

    def test_resolves_selected_remote_tags_with_one_request(self) -> None:
        git = Git(Path("/tmp"))
        output = "tag-object refs/tags/v1\ncommit refs/tags/v1^{}\nother refs/tags/v2"
        with patch.object(git, "run", return_value=output) as run:
            assert git.remote_tag_commits("origin", ["v1", "v2"]) == {"v1": "commit", "v2": "other"}
        run.assert_called_once()

    def test_creates_multiple_tags_and_pushes_explicit_refs_atomically(self) -> None:
        git = Git(Path("/tmp"))
        with patch.object(git, "run") as run:
            git.create_tags("abc", {"v1": "base", "v1.pgall": "variant"})
            git.push_tags("origin", ["v1", "v1.pgall"])
        assert run.call_args_list[-1] == call(
            [
                "push",
                "--atomic",
                "origin",
                "refs/tags/v1:refs/tags/v1",
                "refs/tags/v1.pgall:refs/tags/v1.pgall",
            ],
            mutate=True,
        )
