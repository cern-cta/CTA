# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Render the catalogue's common SQL schema using SQLite metadata and Graphviz."""

import json
import shutil
import sqlite3
import subprocess
from contextlib import closing
from html import escape
from pathlib import Path


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
        'graph [rankdir=RL, bgcolor="white", pad=0.2, nodesep=0.45, ranksep=1.8];',
        'node [shape=plain, fontname="Helvetica", fontsize=14];',
        "edge [penwidth=1.0, arrowsize=0.75, dir=both, arrowtail=box];",
    ]
    for table in tables:
        columns = connection.execute("SELECT name, type, [notnull], pk FROM pragma_table_info(?)", (table,))
        # Graphviz's HTML labels give each column a named port for foreign-key arrows.
        # Label text needs HTML escaping; node and port identifiers use quoted DOT strings.
        lines.append(
            f'{json.dumps(table)} [label=<<TABLE BORDER="1" COLOR="#888888" '
            'CELLBORDER="0" CELLSPACING="0" CELLPADDING="2">'
        )
        lines.append(f'<TR><TD COLSPAN="2" ALIGN="LEFT" BGCOLOR="#f2e6c2"><B>{escape(table)}</B></TD></TR>')
        for name, datatype, not_null, primary_key in columns:
            label = f"<B>{escape(name)}</B>" if primary_key else escape(name)
            description = escape(datatype) + (" NOT NULL" if not_null else "")
            lines.append(
                f'<TR><TD PORT="source_{escape(name)}" ALIGN="LEFT">{label}</TD>'
                f'<TD PORT="target_{escape(name)}" ALIGN="LEFT">{description}</TD></TR>'
            )
        lines.append("</TABLE>>];")
        references = connection.execute(
            'SELECT "table", "from", "to", seq FROM pragma_foreign_key_list(?) ORDER BY id, seq', (table,)
        )
        for parent, source, referenced_column, sequence in references:
            target = referenced_column
            if target is None:
                # REFERENCES parent without a column list targets the parent's primary key in key order.
                target = connection.execute(
                    "SELECT name FROM pragma_table_info(?) WHERE pk = ?", (parent, sequence + 1)
                ).fetchone()[0]
            lines.append(
                f"{json.dumps(table)}:{json.dumps('source_' + source)}:w -> "
                f"{json.dumps(parent)}:{json.dumps('target_' + target)}:e;"
            )
    lines.append("}")
    return "\n".join(lines)


def generate(schema: Path, destination: Path) -> None:
    """Generate an SVG from the pinned schema using the system Graphviz executable."""
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
    dot = shutil.which("dot")
    if dot is None:
        raise FileNotFoundError(
            "Graphviz executable 'dot' not found on PATH. Install Graphviz with dnf install graphviz (EL), "
            "apt install graphviz (Debian/Ubuntu), or brew install graphviz (macOS)."
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603 - Graphviz executable with generated DOT input; no shell.
        [dot, "-Tsvg", "-o", str(destination)], input=source, text=True, encoding="utf-8", check=True
    )
