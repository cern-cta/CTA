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


def stage_content(preview: bool = False) -> None:
    """Assemble source content, linking directories for live preview."""
    destination = ROOT / "build/docs/content"
    if destination.exists():
        shutil.rmtree(destination)
    source = ROOT / "docs/content"
    for directory in (Path("."), Path("api")):
        (destination / directory).mkdir(parents=True, exist_ok=True)
        for entry in (source / directory).iterdir():
            relative = entry.relative_to(source)
            if relative in (Path("api"), Path("api/rust")):
                continue
            target = destination / relative
            if preview:
                target.symlink_to(entry, target_is_directory=entry.is_dir())
            elif entry.is_dir():
                shutil.copytree(entry, target)
            else:
                shutil.copy2(entry, target)


def prepare(skip_rust: bool, preview: bool = False) -> None:
    """Generate references and assemble the documentation input under build/docs."""
    stage_content(preview)
    output = ROOT / "build/docs"
    content = output / "content"
    target = ROOT / "target"
    rust_docs = target / "doc"
    manual = output / "generated/cta-admin.1cta.md"
    manual.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603
        [sys.executable, "compile_man_md.py", "cta-admin.1cta.md.in", str(manual)],
        cwd=ROOT / "tools",
        check=True,
    )
    cargo = shutil.which("cargo")
    if not skip_rust and cargo is None:
        LOGGER.warning("Cargo is not installed; skipping Rust documentation generation.")
        skip_rust = True
    if not skip_rust and cargo is not None:
        LOGGER.info("Generating Rust documentation; use --skip-rust to skip this step.")
        subprocess.run(  # noqa: S603
            [cargo, "doc", "-q", "--no-deps", "--workspace", "--target-dir", str(target)], cwd=ROOT, check=True
        )
    index = rust_docs / "index.html"
    if not index.is_file():
        if skip_rust:
            LOGGER.warning("No %s found; continuing without copying Rust documentation.", index)
            return
        raise FileNotFoundError(f"Rust reference missing after cargo doc: {index}")
    shutil.copytree(rust_docs, content / "api/rust")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "serve"))
    parser.add_argument(
        "--skip-rust", action="store_true", help="Skip Rust generation and reuse target/doc output if available"
    )
    args, extra = parser.parse_known_args()
    os.chdir(ROOT)
    prepare(args.skip_rust, preview=args.command == "serve")
    subprocess.run([sys.executable, "-m", "zensical", args.command, *extra], check=True)  # noqa: S603


if __name__ == "__main__":
    main()
