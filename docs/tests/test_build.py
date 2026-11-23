# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check that builds copy pages, previews reflect edits, and stale Rust output is excluded."""

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
            page.write_text("# Updated")
            self.assertEqual((staged / "dev/index.md").read_text(), "# Original")

            build.stage_content(preview=True)
            page.write_text("# Preview edit")
            self.assertEqual((staged / "dev/index.md").read_text(), "# Preview edit")
            self.assertFalse((staged / "api/rust").exists())


if __name__ == "__main__":
    unittest.main()
