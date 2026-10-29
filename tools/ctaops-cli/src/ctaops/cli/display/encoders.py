# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Encoders for formatting CTA SDK objects for display.

This module provides JSON encoding and field formatting utilities for converting
CTA SDK objects, enums, and complex types into human-readable display formats.
"""

import json
import re
from datetime import date, datetime, timedelta
from enum import Enum
from functools import partial, singledispatch
from ipaddress import IPv4Address, IPv6Address
from typing import Any, Callable, Optional, Protocol

from cta.sdk.units import Byte, BytePerSecond, FreeText, Percentage, Unit
from rich.text import Text


class TableFieldEncoder(Protocol):
    def __call__(self, value: Any, *col_types: type) -> Text: ...


class CtaSdkJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder for CTA SDK objects and types.

    Handles encoding of datetime, date, timedelta, and Enum types
    that are commonly found in CTA SDK model objects.
    """

    def default(self, o):
        """Encode CTA SDK-specific types to JSON-serializable format.

        Args:
            o: Object to encode.

        Returns:
            JSON-serializable representation of the object.

        Raises:
            TypeError: If object type is not recognized.
        """
        if isinstance(o, datetime):
            return int(o.timestamp())
        elif isinstance(o, date):
            return o.isoformat()
        elif isinstance(o, timedelta):
            return int(o.total_seconds())
        elif isinstance(o, Enum):
            return o.name
        elif isinstance(o, (IPv4Address, IPv6Address)):
            return str(o)

        return super().default(o)


def format_bytes(value: float, *, suffix: str = "") -> str:
    """Format a byte value as a human-readable string with appropriate units.

    Converts bytes to KB, MB, GB, TB, PB, or EB based on magnitude.

    Args:
        value: Number of bytes to format.
        suffix: Optional suffix to append to the formatted value (e.g., "/s").

    Returns:
        Formatted byte value with units (e.g., "1.5GB/s").
    """
    if value == 0:
        return f"0B{suffix}"

    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]
    i = 0

    while value >= 1000 and i < len(units) - 1:
        value /= 1000
        i += 1

    return f"{value:.1f}{units[i]}{suffix}"


FieldFormatterFn = Callable[[Any], str]

_unit_encoders: dict[Any, FieldFormatterFn] = {
    Percentage: lambda value: f"{value:.1%}",
    Byte: format_bytes,
    BytePerSecond: partial(format_bytes, suffix="/s"),
}


@singledispatch
def field_encoder(value: Any, unit_type=Optional[type[Unit]]) -> FieldFormatterFn:
    """Dispatch encoder selection based on value type.

    Returns a formatter function appropriate for the given value type.
    This is the base/default implementation that returns str() converter.

    Args:
        value: Example value to determine encoding strategy.
        unit_type: Optional Unit type for specialized formatting.

    Returns:
        Formatter function for the value type.
    """
    return str


@field_encoder.register
def _(value: list, unit_type=Optional[type[Unit]]) -> FieldFormatterFn:
    """Encoder for list values.

    Args:
        value: Example list value.
        unit_type: Optional Unit type.

    Returns:
        Formatter that joins list items with commas.
    """
    return lambda value: ",".join(encode_table_field(item).plain for item in value)


@field_encoder.register
def _(value: int, unit_type=Optional[type[Unit]]) -> FieldFormatterFn:
    """Encoder for integer values.

    Args:
        value: Example integer value.
        unit_type: Optional Unit type for specialized formatting (e.g., Byte, Percentage).

    Returns:
        Formatter function, using unit-specific encoder if available.
    """
    return _unit_encoders.get(unit_type, str)


@field_encoder.register
def _(value: float, unit_type=Optional[type[Unit]]) -> FieldFormatterFn:
    """Encoder for float values.

    Args:
        value: Example float value.
        unit_type: Optional Unit type for specialized formatting.

    Returns:
        Formatter function, using unit-specific encoder if available.
    """
    return _unit_encoders.get(unit_type, lambda value: f"{value:.1f}")


@field_encoder.register
def _(value: datetime, unit_type=Optional[type[Unit]]) -> FieldFormatterFn:
    """Encoder for datetime values.

    Args:
        value: Example datetime value.
        unit_type: Optional Unit type (unused for datetime).

    Returns:
        Formatter that converts datetime to YYYY-MM-DD HH:MM format.
    """
    return lambda value: value.strftime("%Y-%m-%d %H:%M")


@field_encoder.register
def _(value: timedelta, unit_type=Optional[type[Unit]]) -> FieldFormatterFn:
    """Encoder for timedelta values.

    Args:
        value: Example timedelta value.
        unit_type: Optional Unit type (unused for timedelta).

    Returns:
        Formatter that converts timedelta to <>d<>h<>m<>s format.
    """

    return (
        lambda value: (f"{value.days}d" if value.days else "")
        + (f"{value.seconds // 3600}h" if value.total_seconds() >= 3600 else "")
        + (f"{value.seconds % 3600 // 60}m" if value.total_seconds() >= 60 else "")
        + (f"{value.seconds % 60}s" if value.total_seconds() < 60 else "")
    )


@field_encoder.register
def _(value: Enum, unit_type=Optional[type[Unit]]) -> FieldFormatterFn:
    """Encoder for Enum values.

    Args:
        value: Example Enum value.
        unit_type: Optional Unit type (unused for Enum).

    Returns:
        Formatter that returns the enum member name.
    """
    return lambda value: value.name


@field_encoder.register
def _(_: None, unit_type=Optional[type[Unit]]) -> FieldFormatterFn:
    """Encoder for None/null values.

    Args:
        _: None value.
        unit_type: Optional Unit type (unused for None).

    Returns:
        Formatter that returns "-" as placeholder for missing values.
    """
    return lambda value: "-"


def encode_table_field(value: Any, *col_types: type) -> Text:
    """Encode a single field value for table display.

    Selects the appropriate encoder based on value type and optional unit type.

    Args:
        value: Value to encode for display.
        unit_type: Optional Unit type for specialized formatting.

    Returns:
        Formatted string representation of the value.
    """

    return Text(field_encoder(value, next((t for t in col_types if issubclass(t, Unit)), None))(value))


ENCODE_TABLE_HEADER_RULES = [
    (re.compile(r"^tape_session.file_transferred_count$"), "files"),
    (re.compile(r"^start_info.creation_time$"), "start_time"),
    (re.compile(r"^state_change_age$"), "duration"),
    (re.compile(r"^metadata.last_refresh_age$"), "age"),
    (re.compile(r"virtual_organisation_name"), "vo"),
    (re.compile(r"s_count"), "s"),
    (re.compile(r"_count"), "s"),
    (re.compile(r"_name"), ""),
    (re.compile(r"tape_session."), ""),
    (re.compile(r"statistics."), ""),
    (re.compile(r"metadata."), ""),
    (re.compile(r"start_info."), "start_"),
]


def encode_table_header(col_name: str, *col_types: type) -> Text:
    """Encode a column name and return formatting options for table display.

    Processes column name to change some suffixes and prepares
    rich table formatting options.

    Args:
        col_name: Column name (may contain dots for nested fields).
        *col_types: Column types (for potential future extensions).

    Returns:
        Tuple of (Text, formatting_options_dict).
    """
    for pattern, replacement in ENCODE_TABLE_HEADER_RULES:
        col_name = pattern.sub(replacement, col_name)

    return Text(
        col_name.upper(),
        justify="left" if next((t for t in col_types if t is FreeText), None) else "right",
        overflow="fold",
    )
