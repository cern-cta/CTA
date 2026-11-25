# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Exercise schema introspection and rendering with real SQLite and Graphviz."""

# ruff: noqa: PT009, PT027

import base64
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
        self.assertIn("UINT64TYPE", dot)
        self.assertIn("PK, NN", dot)
        self.assertIn("a&amp;b", dot)
        self.assertIn("x&lt;y", dot)
        self.assertIn('"child":"x" -> "parent":"a";', dot)
        self.assertIn('"child":"y" -> "parent":"b";', dot)
        self.assertIn('"child":"other" -> "parent":"a";', dot)
        self.assertEqual(dot.count(" -> "), 3)

    def test_preview_generates_svg_without_writing_to_source(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(build, "ROOT", Path(temporary)):
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
            namespace = {"svg": "http://www.w3.org/2000/svg"}
            style = document.findtext("svg:defs/svg:style", namespaces=namespace)
            self.assertIsNotNone(style)
            assert style is not None
            encoded = style.split("data:font/woff2;base64,", 1)[1].split('"', 1)[0]
            self.assertEqual(
                base64.b64decode(encoded),
                (catalogue_diagram.FONT_DIRECTORY / catalogue_diagram.FONT_FILE).read_bytes(),
            )
            self.assertIn("font-weight: 200 800", style)
            self.assertIn("SIL OPEN FONT LICENSE", document.findtext("svg:metadata", namespaces=namespace) or "")
            for label in document.findall(".//svg:text", namespace):
                self.assertIn(catalogue_diagram.FONT_FAMILY, label.attrib["font-family"])
            self.assertFalse((root / "docs/content/generated").exists())
            schema.write_text("CREATE TABLE replacement (id INTEGER PRIMARY KEY);")
            catalogue_diagram.generate(schema, svg)
            self.assertIn("replacement", svg.read_text())
            self.assertNotIn("UINT64TYPE", svg.read_text())

    def test_missing_schema_is_actionable(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            self.assertRaisesRegex(FileNotFoundError, "git submodule update --init"),
        ):
            catalogue_diagram.generate(Path(temporary) / "missing.sql", Path(temporary) / "out.svg")

    def test_invalid_schema_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            schema = Path(temporary) / "invalid.sql"
            schema.write_text("not SQL")
            with self.assertRaisesRegex(ValueError, "Cannot load catalogue schema"):
                catalogue_diagram.generate(schema, Path(temporary) / "out.svg")

    def test_missing_bundled_font_fails_instead_of_using_host_font(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.object(catalogue_diagram, "FONT_DIRECTORY", Path(temporary)),
            self.assertRaises(FileNotFoundError),
        ):
            catalogue_diagram.render_svg("digraph {}")


if __name__ == "__main__":
    unittest.main()
