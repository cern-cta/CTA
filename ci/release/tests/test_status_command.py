# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from commands import status
from release_config import ReleaseConfig
from release_context import ReleaseContext


class StatusCommandTest(unittest.TestCase):
    """Test read-only status reporting through the shared context."""

    def test_reports_reconstructed_release_state(self) -> None:
        context = ReleaseContext(Path("/tmp"), ReleaseConfig(), MagicMock(), dry_run=False)
        with (
            patch.object(
                context,
                "load_release_context",
                return_value=(
                    {"web_url": "https://gitlab.example/issue"},
                    {"web_url": "https://gitlab.example/mr", "state": "merged"},
                    "abc123",
                ),
            ),
            patch.object(context, "find_pipeline", return_value={"status": "success"}),
            patch.object(context.git, "local_tag_commit", return_value="abc123"),
            patch.object(context.git, "remote_tag_commit", return_value="abc123"),
            redirect_stdout(StringIO()) as output,
        ):
            status.run(context, "v5.12.0.0-1")
        assert "Commit:   abc123" in output.getvalue()
        assert "Pipeline: success" in output.getvalue()
