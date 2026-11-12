# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Prepare generated references, then build or preview the Zensical site."""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def prepare(skip_rust: bool) -> None:
    """Generate the manual and expose Rust reference files as static site assets."""
    manual = ROOT / "build/docs/generated/cta-admin.1cta.md"
    manual.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603
        [sys.executable, "compile_man_md.py", "cta-admin.1cta.md.in", str(manual)],
        cwd=ROOT / "tools",
        check=True,
    )
    if not skip_rust:
        subprocess.run(["cargo", "doc", "-q", "--no-deps", "--workspace"], cwd=ROOT, check=True)  # noqa: S607
    rust = ROOT / "target/doc"
    if not (rust / "index.html").is_file():
        raise FileNotFoundError("Rust reference missing: run without --skip-rust first")
    destination = ROOT / "docs/content/api/rust"
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(rust, destination)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "serve"))
    parser.add_argument("--skip-rust", action="store_true", help="Reuse existing target/doc output")
    args, extra = parser.parse_known_args()
    os.chdir(ROOT)
    prepare(args.skip_rust)
    subprocess.run([sys.executable, "-m", "zensical", args.command, *extra], check=True)  # noqa: S603


if __name__ == "__main__":
    main()
