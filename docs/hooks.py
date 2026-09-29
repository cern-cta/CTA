# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Resolve repository inputs and generate the cta-admin manpage without compiling CTA."""

from pathlib import Path
import re
import subprocess
import sys

from typing import Any, Protocol

from mkdocs.structure.pages import Page
from mkdocs.structure.toc import AnchorLink


class MkDocsConfig(Protocol):
    """The configuration interface used by these hooks."""

    config_file_path: str

    def __getitem__(self, key: str) -> Any: ...


def on_config(config: MkDocsConfig) -> MkDocsConfig:
    # Resolve includes from the repository root so they work regardless of
    # which directory MkDocs is started from.
    root = Path(config.config_file_path).resolve().parent.parent
    config["mdx_configs"]["pymdownx.snippets"]["base_path"] = [str(root)]
    return config


def on_pre_build(config: MkDocsConfig) -> None:
    # Generate the cta-admin manual from this checkout so the documentation
    # stays current without requiring a CTA build.
    root = Path(config.config_file_path).resolve().parent.parent
    output = root / "build/docs/generated/cta-admin.1cta.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603 - Fixed generator from this checkout, with no shell.
        [sys.executable, "compile_man_md.py", "cta-admin.1cta.md.in", str(output)],
        cwd=root / "tools",
        check=True,
    )


def on_page_markdown(markdown: str, config: MkDocsConfig, **kwargs: Any) -> str:
    """Load whole-page manuals without exposing their Pandoc metadata as text."""
    # Strip manual metadata before inclusion so fields such as the date and
    # manual section do not appear as page text.
    match = re.fullmatch(r"\s*--8<--\s*\n([^\n]+\.1cta\.md)\s*\n--8<--\s*", markdown)
    if not match:
        return markdown
    root = Path(config.config_file_path).resolve().parent.parent
    manual = (root / match.group(1).strip()).read_text(encoding="utf-8")
    return re.sub(r"\A---\r?\n.*?\r?\n---(?:\r?\n|\Z)", "", manual, count=1, flags=re.DOTALL)


def on_page_content(html: str, page: Page, **kwargs: Any) -> str:
    """Apply shared diagram defaults and simplify the release-history sidebar."""
    # Mermaid 11 changed arrowhead IDs, so Material leaves them dark in dark mode.
    # Apply the shared arrow color here to keep sequence diagrams readable.
    html = re.sub(
        r'(<pre class="mermaid"><code>)(\s*sequenceDiagram\b)',
        r"\1---\nconfig:\n  themeVariables:\n    signalColor: currentColor\n---\n\2",
        html,
    )
    # Hide repeated category headings from the sidebar to make releases easier
    # to find; the headings remain visible in the page itself.
    if page.file.src_uri == "changelog.md":

        def prune(items: list[AnchorLink]) -> None:
            for item in items:
                if item.level >= 2:
                    item.children = []
                else:
                    prune(item.children)

        prune(page.toc.items)
    return html
