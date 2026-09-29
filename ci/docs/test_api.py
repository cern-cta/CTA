# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check native API generation, failures, and replacement of stale output."""

# ruff: noqa: PT009, PT027

from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from mkdocs.exceptions import PluginError

from api_fixture import prepare_api_fixture


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        prepare_api_fixture(self.root)
        self.hooks = runpy.run_path(str(self.root / "docs/api_hooks.py"))
        self.generate = self.hooks["generate_cpp_api"]
        self.output = self.root / "build/docs/generated/api/cpp"

    def test_native_output_and_replacement(self):
        self.generate(self.root)
        self.assertTrue((self.output / "index.html").is_file())
        widget = (self.output / "class_widget.html").read_text()
        self.assertIn("secret", widget)
        self.assertIn("../../dev/api/", widget)
        self.assertTrue(list(self.output.glob("*_source.html")))
        self.assertTrue((self.output / "search/search.js").is_file())
        self.assertFalse((self.output / "class_excluded_test.html").exists())
        self.assertFalse((self.output / "class_excluded_vendor.html").exists())

        class Config(dict):
            config_file_path = str(self.root / "docs/mkdocs.yml")

        destination = self.root / "site/api/cpp"
        config = Config(site_dir=str(self.root / "site"))
        self.hooks["on_post_build"](config)
        self.assertTrue((destination / "class_widget.html").is_file())
        (self.root / "common/Widget.hpp").unlink()
        self.generate(self.root)
        self.hooks["on_post_build"](config)
        self.assertFalse((self.output / "class_widget.html").exists())
        self.assertFalse((destination / "class_widget.html").exists())

    def test_missing_doxygen(self):
        with patch("shutil.which", return_value=None), self.assertRaisesRegex(PluginError, "Install doxygen"):
            self.generate(self.root)

    def test_failed_doxygen(self):
        with (
            patch("shutil.which", return_value="doxygen"),
            patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "doxygen")),
            self.assertRaisesRegex(PluginError, "generation failed"),
        ):
            self.generate(self.root)

    def test_missing_output(self):
        def write_footer(*args, **kwargs):
            (self.output.parent / "footer.html").write_text("$generatedby")

        with (
            patch("shutil.which", return_value="doxygen"),
            patch("subprocess.run", side_effect=write_footer),
            self.assertRaisesRegex(PluginError, "no C\\+\\+ API index"),
        ):
            self.generate(self.root)
