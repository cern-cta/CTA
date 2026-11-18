# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Prepare generated references, then build or preview the Zensical site."""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

LOGGER = logging.getLogger(__name__)
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
    if not skip_rust and shutil.which("cargo") is None:
        LOGGER.warning("Cargo is not installed; skipping Rust documentation generation.")
        skip_rust = True
    if not skip_rust:
        LOGGER.info("Generating Rust documentation; use --skip-rust to skip this step.")
        subprocess.run(["cargo", "doc", "-q", "--no-deps", "--workspace"], cwd=ROOT, check=True)  # noqa: S607
    rust = ROOT / "target/doc"
    if not (rust / "index.html").is_file():
        if skip_rust:
            LOGGER.warning("No target/doc/index.html found; continuing without copying Rust documentation.")
            return
        raise FileNotFoundError("Rust reference missing after cargo doc: target/doc/index.html")
    destination = ROOT / "docs/content/api/rust"
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(rust, destination)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "serve"))
    parser.add_argument(
        "--skip-rust", action="store_true", help="Skip Rust generation and reuse target/doc output if available"
    )
    args, extra = parser.parse_known_args()
    os.chdir(ROOT)
    prepare(args.skip_rust)
    subprocess.run([sys.executable, "-m", "zensical", args.command, *extra], check=True)  # noqa: S603


if __name__ == "__main__":
    main()
