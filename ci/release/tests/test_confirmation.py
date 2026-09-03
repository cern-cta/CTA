# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confirmation import ConfirmationError, ask_yes_no, confirm


def test_assume_yes_does_not_prompt() -> None:
    with patch("builtins.input") as user_input:
        assert ask_yes_no("Continue?", assume_yes=True)
    user_input.assert_not_called()


def test_dry_run_describes_prompt_without_reading_input(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("builtins.input") as user_input:
        confirm("Publish?", "Declined", dry_run=True)
    user_input.assert_not_called()
    assert "DRY-RUN: would ask: Publish?" in capsys.readouterr().out


def test_noninteractive_confirmation_fails_cleanly() -> None:
    with (
        patch("confirmation.sys.stdin.isatty", return_value=False),
        pytest.raises(ConfirmationError, match="interactive terminal"),
    ):
        confirm("Publish?", "Declined")


def test_eof_fails_cleanly() -> None:
    with (
        patch("confirmation.sys.stdin.isatty", return_value=True),
        patch("builtins.input", side_effect=EOFError),
        pytest.raises(ConfirmationError, match="ended unexpectedly"),
    ):
        confirm("Publish?", "Declined")
