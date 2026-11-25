# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Render the catalogue's common SQL schema using SQLite metadata and Graphviz."""

import base64
import json
import os
import shutil
import sqlite3
import subprocess
from contextlib import closing
from html import escape
from pathlib import Path
from tempfile import TemporaryDirectory

FONT_DIRECTORY = Path(__file__).resolve().parent / "content/assets/fonts/atkinson-next"
FONT_FAMILY = "Atkinson Hyperlegible Next"
FONT_FILE = "AtkinsonHyperlegibleNext.woff2"


def schema_dot(connection: sqlite3.Connection) -> str:
    """Describe columns and foreign-key references without inferring relationship cardinalities."""
    tables = [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT GLOB 'sqlite_*' ORDER BY name"
        )
    ]
    if not tables:
        raise ValueError("Catalogue schema contains no tables")
    lines = [
        "digraph catalogue {",
        'graph [rankdir=LR, bgcolor="white", pad=0.3, nodesep=0.5, ranksep=1.2, pack=true,',
        f'       fontname="{FONT_FAMILY}", labelloc=t,',
        '       label="CTA catalogue\nPK: primary key · NN: not null · arrows reference foreign-key targets"];',
        f'node [shape=plain, fontname="{FONT_FAMILY}", fontsize=10];',
        'edge [color="#64748b", arrowsize=0.6];',
    ]
    for table in tables:
        columns = connection.execute("SELECT name, type, [notnull], pk FROM pragma_table_info(?)", (table,))
        lines.append(f'{json.dumps(table)} [label=<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">')
        lines.append(f'<TR><TD COLSPAN="3" BGCOLOR="#dbeafe"><B>{escape(table)}</B></TD></TR>')
        for name, datatype, not_null, primary_key in columns:
            flags = ", ".join(flag for flag, present in (("PK", primary_key), ("NN", not_null)) if present)
            lines.append(
                f'<TR><TD PORT="{escape(name)}" ALIGN="LEFT">{escape(name)}</TD>'
                f'<TD ALIGN="LEFT">{escape(datatype)}</TD><TD>{flags}</TD></TR>'
            )
        lines.append("</TABLE>>];")
        # Draw each column reference, preserving composite keys and multiple references between tables.
        references = connection.execute(
            'SELECT "table", "from", "to", seq FROM pragma_foreign_key_list(?) ORDER BY id, seq', (table,)
        )
        for parent, source, referenced_column, sequence in references:
            target = referenced_column
            if target is None:
                target = connection.execute(
                    "SELECT name FROM pragma_table_info(?) WHERE pk = ?", (parent, sequence + 1)
                ).fetchone()[0]
            lines.append(f"{json.dumps(table)}:{json.dumps(source)} -> {json.dumps(parent)}:{json.dumps(target)};")
    lines.append("}")
    return "\n".join(lines)


def render_svg(source: str) -> str:
    """Use the bundled font for both Graphviz metrics and standalone browser rendering."""
    dot = shutil.which("dot")
    match = shutil.which("fc-match")
    if dot is None or match is None:
        raise FileNotFoundError(
            "The catalogue diagram requires Graphviz and Fontconfig; install graphviz and fontconfig"
        )
    font = FONT_DIRECTORY / FONT_FILE
    encoded_font = base64.b64encode(font.read_bytes()).decode("ascii")
    license_text = (FONT_DIRECTORY / "OFL.txt").read_text(encoding="utf-8")
    # Isolate layout from host fonts and keep Fontconfig's cache out of the source tree and user configuration.
    with TemporaryDirectory(prefix="cta-diagram-") as temporary:
        config = Path(temporary) / "fonts.conf"
        config.write_text(
            f"<fontconfig><dir>{escape(str(FONT_DIRECTORY))}</dir>"
            f"<cachedir>{escape(temporary)}</cachedir></fontconfig>",
            encoding="utf-8",
        )
        environment = dict(os.environ, FONTCONFIG_FILE=str(config))
        resolved = subprocess.run(  # noqa: S603
            [match, "--format=%{file}", FONT_FAMILY], env=environment, capture_output=True, text=True, check=True
        ).stdout
        if Path(resolved) != font:
            raise RuntimeError(f"Fontconfig could not load the bundled font {font}; resolved {resolved!r}")
        svg = subprocess.run(  # noqa: S603
            [dot, "-Tsvg"], input=source, env=environment, capture_output=True, text=True, check=True
        ).stdout
    # Data URLs also work when the SVG is loaded through <img>, which cannot fetch external fonts.
    embedded = (
        f'<defs><style type="text/css">@font-face {{font-family: "{FONT_FAMILY}"; '
        f'src: url("data:font/woff2;base64,{encoded_font}") format("woff2"); '
        "font-style: normal; font-weight: 200 800;}</style></defs>\n"
        f"<metadata>{escape(license_text)}</metadata>\n"
    )
    return svg.replace("</svg>", embedded + "</svg>")


def generate(schema: Path, destination: Path) -> None:
    """Generate an SVG from the pinned schema; missing inputs or rendering errors fail the build."""
    if not schema.is_file():
        raise FileNotFoundError(
            f"Catalogue schema missing: {schema}. Run git submodule update --init catalogue/cta-catalogue-schema"
        )
    with closing(sqlite3.connect(":memory:")) as connection:
        try:
            connection.executescript(schema.read_text(encoding="utf-8"))
        except sqlite3.Error as error:
            raise ValueError(f"Cannot load catalogue schema {schema}: {error}") from error
        source = schema_dot(connection)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_svg(source), encoding="utf-8")
