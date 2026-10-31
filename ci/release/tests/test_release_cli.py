# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from gitlab_api import GitLabAPIError
from release_cli import create_argument_parser, create_authenticated_api


@pytest.mark.parametrize(
    "arguments",
    [
        ["changelog", "v6.1.0", "--suffix", "pgall"],
        ["changelog", "v6.1.0", "--release-candidate"],
        ["tag", "v6.1.0", "--suffix", "pgall"],
        ["tag"],
        ["status"],
        ["tag", "v6.1.0", "--development", "--release-candidate"],
    ],
)
def test_invalid_arguments(arguments: list[str]) -> None:
    with pytest.raises(SystemExit):
        create_argument_parser().parse_args(arguments)


@pytest.mark.parametrize("command", ["changelog", "status", "tag"])
def test_command_dispatch(command: str) -> None:
    context = MagicMock()
    arguments = create_argument_parser().parse_args([command, "v6.1.0", "--target-branch", "maintenance"])
    with patch(f"release_cli.{command}.run") as run:
        arguments.execute(context, arguments)
    assert run.call_args.args == (
        (context, "v6.1.0", False, "maintenance") if command == "tag" else (context, "v6.1.0", "maintenance")
    )


@pytest.mark.parametrize("dry_run", [False, True])
def test_prompted_token_storage(tmp_path: Path, dry_run: bool) -> None:
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
        assert create_authenticated_api(config, dry_run=dry_run) is api

    api_type.assert_called_once_with("https://gitlab.example", "group/project", "new-token")
    api.authenticate.assert_called_once_with()
    if dry_run:
        assert not token_file.exists()
    else:
        assert token_file.read_text(encoding="utf-8") == "new-token"
        assert token_file.stat().st_mode & 0o777 == 0o600


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


@pytest.mark.parametrize("flag", ["development", "release-candidate"])
def test_prerelease_flag_dispatch(flag: str) -> None:
    context = MagicMock()
    arguments = create_argument_parser().parse_args(["tag", "v6.1.0", "--yes", "--" + flag])
    with patch("release_cli.tag.run") as run:
        arguments.execute(context, arguments)
    assert run.call_args.args[2] is True
    assert run.call_args.kwargs[flag.replace("-", "_")] is True
