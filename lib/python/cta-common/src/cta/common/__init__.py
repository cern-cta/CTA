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

from cta.common.config import NOT_SET, CtaConfigLoader, DataclassWithValidation
from cta.common.dataclasses import copy_dataclass, flatten_dataclass_class, flatten_dataclass_instance, load_data
from cta.common.logging import LoggingConfiguration, configure_logging

__all__ = [
    "NOT_SET",
    "CtaConfigLoader",
    "DataclassWithValidation",
    "LoggingConfiguration",
    "configure_logging",
    "copy_dataclass",
    "flatten_dataclass_class",
    "flatten_dataclass_instance",
    "load_data",
]
