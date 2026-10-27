# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
CTA Operations Utilities Package.

This package provides utility modules for the CERN Tape Archive (CTA) operations
tools. It includes functionality for configuration management, logging, dataclass
operations, and type normalization.

Modules:
    config: Configuration loading and management with validation support.
    logging: Enhanced logging utilities with rotating file handlers and compression.
    dataclasses: Utilities for working with dataclass instances and types.
    typing: Type annotation normalization and manipulation.
"""

from ctaops.utils.config import NOT_SET, CtaOpsConfigLoader, DataclassWithValidation
from ctaops.utils.dataclasses import copy_dataclass, flatten_dataclass_class, flatten_dataclass_instance, load_data
from ctaops.utils.logging import LoggingConfiguration, configure_logging

__all__ = [
    "CtaOpsConfigLoader",
    "NOT_SET",
    "DataclassWithValidation",
    "LoggingConfiguration",
    "configure_logging",
    "load_data",
    "flatten_dataclass_class",
    "flatten_dataclass_instance",
    "copy_dataclass",
]
