# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Regression tests for selecting tapes to label and moving them afterward."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from cta.sdk.client.protocol import CtaClient
from ctaops.cli.commands import tape as tape_commands
from ctaops.cli.config import AppConfig
from ctaops.cli.context import AppContext


@pytest.mark.parametrize("source", ["vid", "file", "pool"])
def test_label_moves_each_selected_tape(source: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = MagicMock(spec=CtaClient)
    tape = SimpleNamespace(vid="V00001", physical_library_name="LIB")
    client.tape.get.return_value = tape
    client.tape.list.return_value = [tape]
    library = MagicMock()
    library.name = "LIB"
    monkeypatch.setattr(tape_commands, "_label_library_for_drive", MagicMock(return_value=library))

    args = ["--drive", "DRIVE", "--dstpool", "destination"]
    if source == "vid":
        args.append(tape.vid)
    elif source == "file":
        tape_list = tmp_path / "tapes.txt"
        tape_list.write_text(f"# Selected tapes\n{tape.vid}\n")
        args.extend(["--file", str(tape_list)])
    else:
        args.extend(["--srcpool", "source"])

    result = CliRunner().invoke(tape_commands.label, args, obj=AppContext(AppConfig(), client))

    assert result.exit_code == 0, result.output
    library.label_tape.assert_called_once_with("DRIVE", tape.vid, old_vid=None, force=False)
    client.tape.update.assert_called_once_with(tape.vid, tape_pool_name="destination")
