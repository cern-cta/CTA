# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Custom dataclass formatters for CTA SDK model types.

This package dynamically loads custom formatter modules that provide
specialized formatting for specific CTA SDK dataclass types. Custom formatters
extend the base formatter to provide domain-specific display logic.

Modules in this package should define formatter classes that inherit from
DataClassBaseFormatter and specify their dataclass_type parameter.
"""

import importlib
import pkgutil


def load_all_formatters() -> None:
    """Load all custom formatters of this package."""
    for m in pkgutil.iter_modules(__path__):
        importlib.import_module(f"{__name__}.{m.name}")
