# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gitlab_api import GitLabAPIError
from release_cli import create_argument_parser, create_authenticated_api


class ArgumentParserTest(unittest.TestCase):
    """Test the small top-level parser and command dispatch boundary."""

    def test_changelog_has_no_tag_family_options(self) -> None:
        parser = create_argument_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["changelog", "v5.12.0.0-1", "--suffix", "pgall"])
        with pytest.raises(SystemExit):
            parser.parse_args(["changelog", "v5.12.0.0-1", "--release-candidate"])

    def test_tag_accepts_repeatable_supported_suffixes(self) -> None:
        args = create_argument_parser().parse_args(["tag", "v5.12.0.0-1", "--suffix", "pgall", "--suffix", "pgsched"])
        assert args.requested_suffixes == ["pgall", "pgsched"]

    def test_tag_parser_rejects_unsupported_suffix(self) -> None:
        with pytest.raises(SystemExit):
            create_argument_parser().parse_args(["tag", "v5.12.0.0-1", "--suffix", "pg"])

    def test_tag_and_status_require_explicit_versions(self) -> None:
        parser = create_argument_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["tag"])
        with pytest.raises(SystemExit):
            parser.parse_args(["status"])

    def test_dispatches_each_command_to_its_module(self) -> None:
        context = MagicMock()
        parser = create_argument_parser()
        with (
            patch("release_cli.changelog.run") as changelog_run,
            patch("release_cli.tag.run") as tag_run,
            patch("release_cli.status.run") as status_run,
        ):
            changelog_args = parser.parse_args(["changelog", "v5.12.0.0-1"])
            changelog_args.execute(context, changelog_args)

            tag_args = parser.parse_args(
                [
                    "tag",
                    "v5.12.0.0-1",
                    "--yes",
                    "--target-branch",
                    "maintenance",
                    "--release-candidate",
                    "--suffix",
                    "pgall",
                ]
            )
            tag_args.execute(context, tag_args)

            status_args = parser.parse_args(["status", "v5.12.0.0-1"])
            status_args.execute(context, status_args)
        changelog_run.assert_called_once_with(context, "v5.12.0.0-1", "main")
        tag_run.assert_called_once_with(
            context,
            "v5.12.0.0-1",
            True,
            "maintenance",
            release_candidate=True,
            requested_suffixes=["pgall"],
        )
        status_run.assert_called_once_with(context, "v5.12.0.0-1", "main")


def test_missing_token_prompts_validates_and_stores(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = MagicMock(gitlab_url="https://gitlab.example", project_id="group/project")
    token_file = tmp_path / "gitlab-api-token"
    api = MagicMock()

    with (
        patch.dict("release_cli.os.environ", {}, clear=True),
        patch("release_cli.TOKEN_FILE", token_file),
        patch("release_cli.sys.stdin.isatty", return_value=True),
        patch("release_cli.getpass.getpass", return_value="new-token"),
        patch("release_cli.ask_yes_no", return_value=True),
        patch("release_cli.GitLabAPI", return_value=api) as api_type,
    ):
        assert create_authenticated_api(config) is api

    api_type.assert_called_once_with("https://gitlab.example", "group/project", "new-token")
    api.authenticate.assert_called_once_with()
    assert token_file.read_text(encoding="utf-8") == "new-token"
    assert token_file.stat().st_mode & 0o777 == 0o600
    assert "https://gitlab.example/-/user_settings/personal_access_tokens" in capsys.readouterr().out


def test_rejected_stored_token_prompts_for_valid_replacement(tmp_path: Path) -> None:
    config = MagicMock(gitlab_url="https://gitlab.example", project_id="group/project")
    token_file = tmp_path / "gitlab-api-token"
    token_file.write_text("expired-token", encoding="utf-8")
    expired_api = MagicMock()
    expired_api.authenticate.side_effect = GitLabAPIError("unauthorized", status_code=401)
    replacement_api = MagicMock()

    with (
        patch.dict("release_cli.os.environ", {}, clear=True),
        patch("release_cli.TOKEN_FILE", token_file),
        patch("release_cli.sys.stdin.isatty", return_value=True),
        patch("release_cli.getpass.getpass", return_value="replacement-token"),
        patch("release_cli.ask_yes_no", return_value=True),
        patch("release_cli.GitLabAPI", side_effect=[expired_api, replacement_api]),
    ):
        assert create_authenticated_api(config) is replacement_api

    expired_api.authenticate.assert_called_once_with()
    replacement_api.authenticate.assert_called_once_with()
    assert token_file.read_text(encoding="utf-8") == "replacement-token"


def test_connection_failure_does_not_prompt_for_replacement(tmp_path: Path) -> None:
    config = MagicMock(gitlab_url="https://gitlab.example", project_id="group/project")
    token_file = tmp_path / "gitlab-api-token"
    token_file.write_text("token", encoding="utf-8")
    api = MagicMock()
    api.authenticate.side_effect = GitLabAPIError("connection refused")

    with (
        patch.dict("release_cli.os.environ", {}, clear=True),
        patch("release_cli.TOKEN_FILE", token_file),
        patch("release_cli.getpass.getpass") as getpass,
        patch("release_cli.GitLabAPI", return_value=api),
        pytest.raises(GitLabAPIError, match="connection refused"),
    ):
        create_authenticated_api(config)

    getpass.assert_not_called()


def test_dry_run_validates_prompted_token_without_storing_it(tmp_path: Path) -> None:
    config = MagicMock(gitlab_url="https://gitlab.example", project_id="group/project")
    token_file = tmp_path / "gitlab-api-token"
    api = MagicMock()

    with (
        patch.dict("release_cli.os.environ", {}, clear=True),
        patch("release_cli.TOKEN_FILE", token_file),
        patch("release_cli.sys.stdin.isatty", return_value=True),
        patch("release_cli.getpass.getpass", return_value="new-token"),
        patch("release_cli.ask_yes_no") as ask_yes_no,
        patch("release_cli.GitLabAPI", return_value=api),
    ):
        assert create_authenticated_api(config, dry_run=True) is api

    api.authenticate.assert_called_once_with()
    ask_yes_no.assert_not_called()
    assert not token_file.exists()
