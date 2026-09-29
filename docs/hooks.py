# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Generate manuals and native API documentation without compiling CTA."""

from pathlib import Path
import re
import shutil
import subprocess
import sys

from typing import Any, Protocol

from mkdocs.structure.pages import Page
from mkdocs.structure.toc import AnchorLink
from mkdocs.exceptions import PluginError


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
    generate_cpp_api(root)


def generate_cpp_api(root: Path) -> None:
    """Build a fresh, self-contained native HTML site from the checkout."""
    executable = shutil.which("doxygen")
    if executable is None:
        raise PluginError("Doxygen is required to build the docs. Install doxygen and rerun MkDocs.")
    output = root / "build/docs/generated/api/cpp"
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    try:
        # Retain the installed Doxygen version's footer structure and tree-view
        # elements instead of maintaining a version-specific HTML template.
        footer = output.parent / "footer.html"
        subprocess.run(  # noqa: S603 - Trusted executable with build-local output paths.
            [
                executable,
                "-w",
                "html",
                str(output.parent / "header.html"),
                str(footer),
                str(output.parent / "style.css"),
            ],
            cwd=root,
            check=True,
        )
        return_link = (root / "docs/api/footer.html").read_text(encoding="utf-8")
        footer.write_text(
            footer.read_text(encoding="utf-8").replace("$generatedby", return_link + "$generatedby"), encoding="utf-8"
        )
        subprocess.run(  # noqa: S603 - Trusted executable and checked-in configuration, without a shell.
            [executable, "docs/api/Doxyfile"], cwd=root, check=True
        )
    except subprocess.CalledProcessError as error:
        raise PluginError("C++ API generation failed; see the Doxygen diagnostics above.") from error
    if not (output / "index.html").is_file():
        raise PluginError("Doxygen produced no C++ API index; check docs/api/Doxyfile.")


def on_post_build(config: MkDocsConfig) -> None:
    """Mount each native API site inside the current MkDocs version."""
    root = Path(config.config_file_path).resolve().parent.parent
    # Add future language outputs here; preserve their complete HTML trees.
    for language in ("cpp",):
        source = root / "build/docs/generated/api" / language
        destination = Path(config["site_dir"]) / "api" / language
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(source, destination)


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
