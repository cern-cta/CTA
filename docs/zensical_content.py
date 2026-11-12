# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Resolve CTA repository content and diagram defaults during Markdown rendering."""

import json
import re
import sys
from pathlib import Path
from typing import Any

from markdown import Markdown
from markdown.extensions import Extension
from markdown.postprocessors import Postprocessor
from markdown.preprocessors import Preprocessor

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

ROOT = Path(__file__).resolve().parent.parent


def repository_content(text: str, root: Path) -> str:
    """Resolve the EOS image tag and strip Pandoc metadata from included manuals."""
    if "{{ eos_image_tag }}" in text:
        project = json.loads((root / "project.json").read_text(encoding="utf-8"))
        text = text.replace("{{ eos_image_tag }}", project["dev"]["eosImageTag"])
    match = re.fullmatch(r"\s*--8<--\s*\n([^\n]+\.1cta\.md)\s*\n--8<--\s*", text)
    if match:
        manual = (root / match.group(1).strip()).read_text(encoding="utf-8")
        return re.sub(r"\A---\r?\n.*?\r?\n---(?:\r?\n|\Z)", "", manual, count=1, flags=re.DOTALL)
    return text


class RepositoryContent(Preprocessor):
    @override
    def run(self, lines: list[str]) -> list[str]:
        return repository_content("\n".join(lines), ROOT).splitlines()


class DiagramDefaults(Postprocessor):
    @override
    def run(self, text: str) -> str:
        # Sequence arrowheads need currentColor to follow the selected palette.
        return re.sub(
            r'(<pre class="mermaid"><code>)(\s*sequenceDiagram\b)',
            r"\1---\nconfig:\n  themeVariables:\n    signalColor: currentColor\n---\n\2",
            text,
        )


class CTAContent(Extension):
    @override
    def extendMarkdown(self, md: Markdown) -> None:
        md.preprocessors.register(RepositoryContent(md), "cta_repository", 35)
        md.postprocessors.register(DiagramDefaults(md), "cta_diagrams", 0)


def makeExtension(**kwargs: Any) -> CTAContent:  # noqa: N802 - name required by Python-Markdown
    return CTAContent(**kwargs)
