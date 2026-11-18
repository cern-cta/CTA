# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check that build copies and preview symlinks leave the original documentation files unchanged."""

# ruff: noqa: PT009

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from docs import build


class StagingTests(unittest.TestCase):
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
            self.assertFalse((staged / "dev").is_symlink())
            page.write_text("# Updated")
            self.assertEqual((staged / "dev/index.md").read_text(), "# Original")

            build.stage_content(preview=True)
            self.assertTrue((staged / "dev").is_symlink())
            self.assertEqual((staged / "dev/index.md").read_text(), "# Updated")
            added = source / "dev/new.md"
            added.write_text("# New")
            self.assertEqual((staged / "dev/new.md").read_text(), "# New")
            added.unlink()
            self.assertFalse((staged / "dev/new.md").exists())

            build.stage_content()
            self.assertFalse((staged / "dev").is_symlink())
            self.assertEqual((staged / "dev/index.md").read_text(), "# Updated")
            self.assertEqual(legacy.read_text(), "Stale Rust output")


if __name__ == "__main__":
    unittest.main()
