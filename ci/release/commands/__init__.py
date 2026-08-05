# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Command implementations for the CTA release CLI."""

from __future__ import annotations

import argparse
from typing import Any, Protocol


class SubparserRegistry(Protocol):
    """Describe the public parser operation needed to register a command."""

    def add_parser(self, name: str, **kwargs: Any) -> argparse.ArgumentParser:
        """Create and return a named command parser."""
        ...
