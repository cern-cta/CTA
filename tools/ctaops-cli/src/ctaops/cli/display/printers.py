# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Printers for outputting dataclass objects in various formats.

This module provides functions for printing dataclass objects in different
output formats: raw (string representation), JSON, and table display.
"""

from collections.abc import Iterable
from dataclasses import is_dataclass
from itertools import chain, islice
from typing import Optional, Union

from ctaops.cli.config import DisplayConfig
from rich import print, print_json
from rich.console import Console
from rich.table import Table

from .dataclass_formatters import load_all_formatters
from .formatters import DataClassFormatterProvider, DataClassProtocol

load_all_formatters()


def raw_print(objects: Union[DataClassProtocol, Iterable[DataClassProtocol]]) -> None:
    """Print dataclass objects in raw string format.

    Args:
        objects: A single dataclass object or sequence of dataclass objects.
    """
    if not isinstance(objects, Iterable):
        return print(objects)

    iterator = iter(objects)

    try:
        first = next(iterator)
    except StopIteration:
        print("[bold][red]No data[/red][/bold]")
        return

    print(str(first))

    for obj in iterator:
        print(str(obj))


def print_as_json(objects: Union[DataClassProtocol, Iterable[DataClassProtocol]]) -> None:
    """Print dataclass objects in JSON format.

    Each object is output as a separate JSON line (JSONL format).

    Args:
        objects: A single dataclass object or sequence of dataclass objects.
    """
    if not isinstance(objects, Iterable):
        print_json(DataClassFormatterProvider.get(type(objects)).to_json(objects))
        return

    iterator = iter(objects)

    try:
        first = next(iterator)
    except StopIteration:
        print()
        return

    formatter = DataClassFormatterProvider.get(type(first))

    print_json(formatter.to_json(first))

    for obj in iterator:
        print_json(formatter.to_json(obj))


def print_as_table(
    objects: Union[DataClassProtocol, Iterable[DataClassProtocol]],
    title: Optional[str] = None,
    display_config: Optional[DisplayConfig] = None,
) -> None:
    """Print dataclass objects in table format.

    For a single object, prints a key-value table. For multiple objects,
    prints a columnar table with one row per object.

    Args:
        objects: A single dataclass object or sequence of dataclass objects.
        format: Output format - "table" or "long-table".
        title: Optional title for the table.
        chunk_size: Number of objects to include in each chunk for display.
    """
    display_config = display_config or DisplayConfig()

    if display_config.pretty_get and is_dataclass(objects):
        return print(DataClassFormatterProvider.get(type(objects)).to_table(objects))

    console = Console(width=1000)

    if not isinstance(objects, Iterable):
        objects = [objects]

    iterator = iter(objects)

    # Check if there is data or not at all
    if not (first_chunk := list(islice(iterator, display_config.chunk_size))):
        return console.print("[bold red]No data[/bold red]")

    # Check if there is more than 1 chunk or not.
    if (new_obj := next(iterator, None)) is None:
        chunk = None
    else:
        iterator = chain([new_obj], iterator)
        chunk = first_chunk

    exclude_fields = next(
        (
            tuple(col_config.exclude_fields)
            for col_config in display_config.columns
            if col_config.table_name == type(first_chunk[0]).__name__
        ),
        (),
    )

    dataclass_formatter = DataClassFormatterProvider.get(type(first_chunk[0]), *exclude_fields)

    table_headers = dataclass_formatter.table_headers()
    table_columns_parameters = dataclass_formatter.table_columns_parameters(chunk=chunk)

    def make_table(chunk: Iterable[DataClassProtocol], *, show_header: bool) -> Table:
        table = Table(
            box=None,
            show_edge=False,
            show_header=show_header,
            padding=(0, 1, 0, 0),
            title=title,
        )

        for header, params in zip(table_headers, table_columns_parameters):
            table.add_column(header, **params)

        for obj in chunk:
            table.add_row(*(dataclass_formatter.table_entry(obj)))

        return table

    console.print(make_table(first_chunk, show_header=True), markup=False)
    while chunk := list(islice(iterator, display_config.chunk_size)):
        console.print(make_table(chunk, show_header=False), markup=False)


def print_dataclasses(
    objects: Union[DataClassProtocol, Iterable[DataClassProtocol]],
    display_config: Optional[DisplayConfig] = None,
    title: Optional[str] = None,
):
    """Print dataclass objects in the specified format.

    This is the main entry point for printing dataclass objects.
    Dispatches to the appropriate printer based on the format parameter.

    Args:
        objects: A single dataclass object or iterable of dataclass objects.
        display_config: Configuration for display settings.
        title: Optional title for the table.

    Raises:
        ValueError: If format is not one of the supported values.
    """
    display_config = display_config or DisplayConfig()

    if display_config.format == "raw":
        raw_print(objects)
    elif display_config.format == "json":
        print_as_json(objects)
    elif display_config.format == "table":
        print_as_table(objects, title=title, display_config=display_config)
    else:
        raise ValueError(
            f"format {display_config.format} was provided where only {{'raw','table','json','long-table'}} are allowed"
        )
