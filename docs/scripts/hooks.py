# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Resolve repository inputs and generate the cta-admin manpage without compiling CTA."""

from pathlib import Path
import subprocess
import sys


def on_config(config):
    # Resolve snippets from the config location, independent of the launch directory.
    root = Path(config.config_file_path).resolve().parent.parent
    config["mdx_configs"]["pymdownx.snippets"]["base_path"] = [str(root)]
    return config


def on_pre_build(config):
    root = Path(config.config_file_path).resolve().parent.parent
    output = root / "build/docs/generated/cta-admin.1cta.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603 - Fixed generator from this checkout, with no shell.
        [sys.executable, "compile_man_md.py", "cta-admin.1cta.md.in", str(output)],
        cwd=root / "tools",
        check=True,
    )
