# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Resolve API sources independently of the caller's working directory."""

from pathlib import Path

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.plugins import event_priority


@event_priority(100)
def on_config(config: MkDocsConfig) -> MkDocsConfig:
    # Run before mkdocstrings caches its handler configuration.
    root = Path(config.config_file_path).resolve().parents[2]
    handler = config["plugins"]["mkdocstrings"].config.handlers["python"]
    handler["paths"] = [
        str(root / path)
        for path in (
            "lib/python/cta-sdk/src",
            "lib/python/ctaops-sdk/src",
            "lib/python/ctaops-utils/src",
            "tools/ctaops-cli/src",
        )
    ]
    return config
