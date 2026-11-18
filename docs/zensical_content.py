# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Prepare CTA manuals, examples, and diagrams for display on the documentation site.

Zensical loads this Markdown extension through zensical.toml, both for local builds
and when mike rebuilds the site for publication. The manual and Rust documentation
files are generated separately by docs/build.py.
"""

import json
import re
from pathlib import Path
from typing import Any, override

from markdown import Markdown
from markdown.extensions import Extension
from markdown.postprocessors import Postprocessor
from markdown.preprocessors import Preprocessor

ROOT = Path(__file__).resolve().parent.parent


def repository_content(text: str, root: Path) -> str:
    """Fill in the EOS image version and remove manual metadata that should not appear on the website."""
    # Use the same EOS image version in documentation examples as in the development environment.
    if "{{ eos_image_tag }}" in text:
        project = json.loads((root / "project.json").read_text(encoding="utf-8"))
        text = text.replace("{{ eos_image_tag }}", project["dev"]["eosImageTag"])
    # Some pages contain only an instruction to include a command manual.
    # Read that manual here and remove its opening YAML metadata, which Pandoc uses to build man pages.
    match = re.fullmatch(r"\s*--8<--\s*\n([^\n]+\.1cta\.md)\s*\n--8<--\s*", text)
    if match:
        manual = (root / match.group(1).strip()).read_text(encoding="utf-8")
        return re.sub(r"\A---\r?\n.*?\r?\n---(?:\r?\n|\Z)", "", manual, count=1, flags=re.DOTALL)
    return text


class RepositoryContent(Preprocessor):
    """Update examples and load command manuals before Markdown is converted to HTML."""

    @override
    def run(self, lines: list[str]) -> list[str]:
        return repository_content("\n".join(lines), ROOT).splitlines()


class DiagramDefaults(Postprocessor):
    """Make Mermaid sequence-diagram arrows readable in both light and dark mode."""

    @override
    def run(self, text: str) -> str:
        # Use the surrounding text color for arrows. Leave diagrams with their own configuration unchanged.
        return re.sub(
            r'(<pre class="mermaid"><code>)(\s*sequenceDiagram\b)',
            r"\1---\nconfig:\n  themeVariables:\n    signalColor: currentColor\n---\n\2",
            text,
        )


class CTAContent(Extension):
    @override
    def extendMarkdown(self, md: Markdown) -> None:
        # Run before the snippets extension replaces include instructions with file contents.
        # This lets us recognize pages that include a manual and remove the manual metadata.
        md.preprocessors.register(RepositoryContent(md), "cta_repository", 35)
        # Run after Markdown conversion, when Mermaid diagrams are wrapped in HTML <pre><code> elements.
        md.postprocessors.register(DiagramDefaults(md), "cta_diagrams", 0)


def makeExtension(**kwargs: Any) -> CTAContent:  # noqa: N802 - name required by Python-Markdown
    """Create the extension when Python-Markdown loads it from zensical.toml."""
    return CTAContent(**kwargs)
