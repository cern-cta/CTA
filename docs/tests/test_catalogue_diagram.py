# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Exercise schema introspection and rendering with real SQLite and Graphviz."""

# ruff: noqa: PT009, PT027

import os
import sqlite3
import tempfile
import unittest
import xml.etree.ElementTree as ET
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from docs import build, catalogue_diagram

SCHEMA = """
CREATE TABLE parent (a UINT64TYPE NOT NULL, b VARCHAR(20), PRIMARY KEY (a, b));
CREATE TABLE child (
    id INTEGER PRIMARY KEY,
    x UINT64TYPE, y VARCHAR(20), other UINT64TYPE,
    FOREIGN KEY (x, y) REFERENCES parent,
    FOREIGN KEY (other) REFERENCES parent(a)
);
CREATE TABLE "a&b" ("x<y" VARCHAR(10));
"""


class CatalogueDiagramTests(unittest.TestCase):
    def test_columns_and_distinct_composite_references(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.executescript(SCHEMA)
            dot = catalogue_diagram.schema_dot(connection)
        self.assertIn("UINT64TYPE NOT NULL", dot)
        self.assertIn("<B>a</B>", dot)
        self.assertIn("a&amp;b", dot)
        self.assertIn("x&lt;y", dot)
        self.assertIn('"child":"source_x":w -> "parent":"target_a":e;', dot)
        self.assertIn('"child":"source_y":w -> "parent":"target_b":e;', dot)
        self.assertIn('"child":"source_other":w -> "parent":"target_a":e;', dot)
        self.assertEqual(dot.count(" -> "), 3)

    def test_preview_generates_svg_without_writing_to_source(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.object(build, "ROOT", Path(temporary)),
        ):
            root = Path(temporary)
            (root / "docs/content/api").mkdir(parents=True)
            schema = root / "schema.sql"
            schema.write_text(SCHEMA)
            build.stage_content(preview=True)
            svg = root / "build/docs/content/generated/catalogue-schema.svg"
            catalogue_diagram.generate(schema, svg)
            document = ET.parse(svg)  # noqa: S314 - locally generated Graphviz output
            self.assertTrue(document.getroot().tag.endswith("svg"))
            self.assertIn("UINT64TYPE", " ".join(document.getroot().itertext()))
            self.assertFalse((root / "docs/content/generated").exists())

    def test_missing_graphviz_is_actionable(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {"PATH": ""}):
            schema = Path(temporary) / "schema.sql"
            schema.write_text(SCHEMA)
            with self.assertRaisesRegex(FileNotFoundError, "Install Graphviz"):
                catalogue_diagram.generate(schema, Path(temporary) / "out.svg")

    def test_missing_schema_is_actionable(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            self.assertRaisesRegex(FileNotFoundError, "git submodule update --init"),
        ):
            catalogue_diagram.generate(Path(temporary) / "missing.sql", Path(temporary) / "out.svg")


if __name__ == "__main__":
    unittest.main()
