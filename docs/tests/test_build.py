# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check that builds copy pages, previews reflect edits, and stale Rust output is excluded."""

# ruff: noqa: PT009, PT027

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from docs import build


class StagingTests(unittest.TestCase):
    def test_supplied_rust_artifact_is_copied_without_cargo(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(build, "ROOT", Path(temporary)):
            root = Path(temporary)
            (root / "docs/content/api").mkdir(parents=True)
            rust = root / "artifact"
            (rust / "static.files").mkdir(parents=True)
            (rust / "index.html").write_text("Rust index")
            (rust / "static.files/main.js").write_text("Rust assets")
            with patch.object(build.subprocess, "run") as run, patch.object(build.shutil, "which", return_value=None):
                build.prepare(False, rust_docs=rust)
            # Only the Python manual generator runs in the assembly job.
            self.assertEqual(run.call_count, 1)
            self.assertIn(str(root / "ci/build/compile_man_md.py"), run.call_args.args[0])
            staged = root / "build/docs/content/api/rust"
            self.assertEqual((staged / "index.html").read_text(), "Rust index")
            self.assertEqual((staged / "static.files/main.js").read_text(), "Rust assets")

    def test_missing_rust_artifact_does_not_fall_back_to_local_output(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(build, "ROOT", Path(temporary)):
            root = Path(temporary)
            (root / "docs/content/api").mkdir(parents=True)
            (root / "target/doc").mkdir(parents=True)
            (root / "target/doc/index.html").write_text("Stale Rust output")
            with self.assertRaisesRegex(FileNotFoundError, "Rust documentation artifact is missing"):
                build.prepare(False, rust_docs=root / "missing-artifact")

    def test_build_copies_and_preview_links_authored_content(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(build, "ROOT", Path(temporary)):
            source = Path(temporary) / "docs/content"
            staged = Path(temporary) / "build/docs/content"
            page = source / "dev/index.md"
            page.parent.mkdir(parents=True)
            page.write_text("# Original")
            legacy = source / "api/rust/index.html"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("Stale Rust output")
            build.stage_content()
            self.assertFalse((staged / "api/rust").exists())
            page.write_text("# Updated")
            self.assertEqual((staged / "dev/index.md").read_text(), "# Original")

            build.stage_content(preview=True)
            page.write_text("# Preview edit")
            self.assertEqual((staged / "dev/index.md").read_text(), "# Preview edit")
            self.assertFalse((staged / "api/rust").exists())


if __name__ == "__main__":
    unittest.main()
