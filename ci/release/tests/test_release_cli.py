# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from release_cli import create_argument_parser


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
                    "--ref",
                    "main",
                    "--release-candidate",
                    "--suffix",
                    "pgall",
                ]
            )
            tag_args.execute(context, tag_args)

            status_args = parser.parse_args(["status", "v5.12.0.0-1"])
            status_args.execute(context, status_args)
        changelog_run.assert_called_once_with(context, "v5.12.0.0-1")
        tag_run.assert_called_once_with(
            context,
            "v5.12.0.0-1",
            True,
            "main",
            release_candidate=True,
            requested_suffixes=["pgall"],
        )
        status_run.assert_called_once_with(context, "v5.12.0.0-1")
