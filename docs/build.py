# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Prepare the command manual and Rust API documentation, then build or preview the website."""

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
    """Copy pages into build/docs/content, or symlink them so preview picks up edits."""
    destination = ROOT / "build/docs/content"
    if destination.exists():
        shutil.rmtree(destination)
    source = ROOT / "docs/content"
    # Keep api/ as a real directory so generated Rust HTML can be added beside the Python reference pages.
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


def prepare(skip_rust: bool, preview: bool = False, rust_docs: Path | None = None) -> None:
    """Prepare pages, the command manual, and Rust HTML for Zensical to read from build/docs."""
    stage_content(preview)
    output = ROOT / "build/docs"
    content = output / "content"
    target = ROOT / "target"
    # CI supplies a required artifact; local previews may generate or omit the reference.
    supplied_rust = rust_docs is not None
    if rust_docs is None:
        rust_docs = target / "doc"
    if supplied_rust and not (rust_docs / "index.html").is_file():
        raise FileNotFoundError(f"Rust documentation artifact is missing: {rust_docs / 'index.html'}")
    manual = output / "generated/cta-admin.1cta.md"
    manual.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603
        [sys.executable, "compile_man_md.py", "cta-admin.1cta.md.in", str(manual)],
        cwd=ROOT / "tools",
        check=True,
    )
    skip_rust = skip_rust or supplied_rust
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
    rust = parser.add_mutually_exclusive_group()
    rust.add_argument(
        "--skip-rust", action="store_true", help="Skip Rust generation and reuse target/doc output if available"
    )
    rust.add_argument(
        "--rust-docs", type=Path, help="Assemble an existing Rust documentation artifact (required to exist)"
    )
    args, extra = parser.parse_known_args()
    os.chdir(ROOT)
    prepare(args.skip_rust, preview=args.command == "serve", rust_docs=args.rust_docs)
    subprocess.run([sys.executable, "-m", "zensical", args.command, *extra], check=True)  # noqa: S603


if __name__ == "__main__":
    main()
