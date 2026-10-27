# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Formatters for converting dataclass objects to display formats.

This module handles conversion of dataclass instances to JSON and Rich table
formats, with support for nested dataclasses, custom formatters, and unit-aware
field formatting.
"""

import json
from dataclasses import asdict, fields, is_dataclass
from typing import (
    Any,
    ClassVar,
    Generic,
    Optional,
    Protocol,
    TypeVar,
    cast,
)

from cta.sdk.units import FreeText
from ctaops.utils.dataclasses import flatten_dataclass_class, flatten_dataclass_instance
from rich.table import Table
from rich.text import Text

from ctaops.cli.display.encoders import CtaSdkJsonEncoder, encode_table_field, encode_table_header


class DataClassProtocol(Protocol):
    """Protocol for dataclass objects.

    Attributes:
        __dataclass_fields__: Dictionary of dataclass field definitions.
    """

    __dataclass_fields__: ClassVar[dict]


D = TypeVar("D", bound=DataClassProtocol)


def dataclass_to_rich_table(
    obj: DataClassProtocol, title: Optional[str] = None, max_depth: int = 5, _current_depth: int = 0
) -> Table:
    """Convert a dataclass object to a Rich table for display.

    Creates a formatted table showing all fields and values of a dataclass,
    including nested dataclasses in sub-tables. Limits recursion depth to
    prevent infinite loops with circular references.

    Args:
        obj: Dataclass instance to convert.
        title: Optional title for the table (defaults to class name).
        max_depth: Maximum nesting depth before stopping recursion.
        _current_depth: Current recursion depth (internal use only).

    Returns:
        Rich Table object formatted for display.

    Raises:
        ValueError: If obj is not a dataclass instance.
    """
    if not is_dataclass(obj):
        raise ValueError("Object must be a dataclass instance")

    if _current_depth >= max_depth:
        return Text(f"[yellow]Max depth ({max_depth}) reached[/yellow]")  # type: ignore

    table = Table(
        title=title or f"{type(obj).__name__}", show_header=True, header_style="bold magenta", border_style="blue"
    )

    table.add_column("Field", style="cyan", no_wrap=True)
    table.add_column("Value", style="green")

    for field in fields(obj):
        field_name = field.name
        field_value = getattr(obj, field_name)

        if is_dataclass(field_value):
            nested_table = dataclass_to_rich_table(
                cast(DataClassProtocol, field_value),
                title=f"{field_name}",
                max_depth=max_depth,
                _current_depth=_current_depth + 1,  # type: ignore
            )
            table.add_row(field_name, nested_table)

        elif isinstance(field_value, list) and field_value and is_dataclass(field_value[0]):
            list_table = Table(title=f"{field_name} (List)", show_header=False, border_style="dim blue", padding=(0, 1))
            list_table.add_column("Items")

            for i, item in enumerate(field_value):
                item_table = dataclass_to_rich_table(
                    item, title=f"[{i}]", max_depth=max_depth, _current_depth=_current_depth + 1
                )
                list_table.add_row(item_table)

            table.add_row(field_name, list_table)

        else:
            table.add_row(field_name, str(field_value))

    return table


TableEntry = list[Text]
TableHeaders = list[Text]
TableColumnsParameters = list[dict[str, Any]]


class DataClassBaseFormatter(Generic[D]):
    """Base formatter class for converting dataclass objects to various formats.

    This class provides default formatting implementations and a registration
    mechanism for custom formatters per dataclass type. Subclasses can override
    methods to provide specialized formatting.

    Type Parameters:
        D: Dataclass type being formatted.
    """

    def __init_subclass__(cls, dataclass_type: type[D], **kwargs) -> None:
        """Register subclass as formatter for specified dataclass type.

        Args:
            dataclass_type: The dataclass type this formatter handles.
            **kwargs: Additional keyword arguments passed to parent.
        """
        DataClassFormatterProvider.register(dataclass_type, cls)

    def __init__(self, dataclass_type: type[D], *exclude_fields: str):
        """Initialize formatter for a dataclass type.

        Args:
            dataclass_type: The dataclass type to format.
        """
        self.dataclass_type = dataclass_type
        self.exclude_fields = exclude_fields

    def to_json(self, obj: D) -> str:
        """Convert dataclass object to JSON string.

        Args:
            obj: Dataclass object to convert.

        Returns:
            JSON string representation.
        """
        return json.dumps(asdict(obj), cls=CtaSdkJsonEncoder)

    def to_table(self, obj: D) -> Table:
        """Convert dataclass object to Rich table.

        Args:
            obj: Dataclass object to convert.

        Returns:
            Rich Table for display.
        """
        return dataclass_to_rich_table(obj)

    def _default_raw_columns_names(self) -> list[str]:
        return [
            name
            for name, _ in flatten_dataclass_class(
                self.dataclass_type,
                max_depth=1,
                extra_flatten_fields=("metadata.comment",),  # always try to include this field by default
                include_properties=True,
            )
        ]

    def _raw_columns_names(self) -> list[str]:
        """Get raw column names (ordered) for the dataclass.

        Returns:
            List of str (depth level are separated by a `.`).
        """
        return [name for name in self._default_raw_columns_names() if name not in self.exclude_fields]

    def _raw_columns_types(self) -> list[frozenset[type]]:
        """Get raw column types (ordered) for the dataclass.

        Returns:
            List of frozenset[type] (Union of every possible type).
        """
        columns = flatten_dataclass_class(
            self.dataclass_type,
            max_depth=None,  # import everything to allow to make it work with override of _raw_columns_names method
            include_properties=True,
        )

        # we keep the same order (.raw_columns_names as the source of truth)
        # like this we can override _raw_columns_names method, the _raw_columns_types will reflect the changes
        return [
            col_types
            for col_name in self._raw_columns_names()
            if (col_types := next((ct for cn, ct in columns if cn == col_name), None)) is not None
        ]

    def _raw_table_entry(self, obj: D) -> list[Any]:
        """Get raw field value (ordered) for the dataclass.

        Returns:
            List of objects.
        """
        flat_obj = flatten_dataclass_instance(
            obj,
            max_depth=None,
            include_properties=True,
        )

        # raw columns name as source of truth for selection and ordering
        return [flat_obj.get(col_name, None) for col_name in self._raw_columns_names()]

    def table_headers(self) -> TableHeaders:
        """Get Rich-formatted column definitions for table display.

        Returns:
            List of tuples (column_name, formatting_options_dict).
        """

        return [
            encode_table_header(col_name, *col_types)
            for col_name, col_types in zip(self._raw_columns_names(), self._raw_columns_types())
        ]

    def table_columns_parameters(self, chunk: Optional[list[D]] = None) -> TableColumnsParameters:
        """Get Rich-formatted column definitions for table display.

        Returns:
            List of tuples (column_name, formatting_options_dict).
        """
        table_entries = [self.table_entry(o) for o in chunk] if chunk else []
        columns_parameters: list[dict[str, Any]] = []
        default_parameters = {"overflow": "fold", "justify": "right"}

        for column_index, (header, col_types) in enumerate(zip(self.table_headers(), self._raw_columns_types())):
            override_parameters: dict[str, Any] = {}
            override_parameters |= {"justify": "left"} if any(ct is FreeText for ct in col_types) else {}
            override_parameters |= (
                {"width": max(*(len(entry[column_index]) for entry in table_entries), len(header))}
                if table_entries
                else {}
            )
            columns_parameters.append(default_parameters | override_parameters)

        return columns_parameters

    def table_entry(self, obj: D) -> TableEntry:
        """Generate formatted field values for a table row.

        Args:
            obj: Dataclass object to extract values from.

        Returns:
            Formatted string representation of each field value.
        """
        return [
            encode_table_field(value, *col_types)
            for value, col_types in zip(self._raw_table_entry(obj), self._raw_columns_types())
        ]


class DataClassFormatterProvider:
    """Registry and factory for dataclass formatters.

    Manages registration of custom formatters for specific dataclass types
    and caches formatter instances for performance.
    """

    _registry: dict[type[DataClassProtocol], type[DataClassBaseFormatter]] = {}
    _formatters_cache: dict[type[DataClassProtocol], DataClassBaseFormatter] = {}

    @classmethod
    def register(cls, dataclass_type: type[DataClassProtocol], custom_formatter: type[DataClassBaseFormatter]) -> None:
        """Register a custom formatter for a dataclass type.

        Args:
            dataclass_type: The dataclass type to register a formatter for.
            custom_formatter: The formatter class for this type.
        """
        cls._registry.setdefault(dataclass_type, custom_formatter)

    @classmethod
    def get(cls, dataclass_type: type[DataClassProtocol], *exclude_fields: str) -> DataClassBaseFormatter:
        """Get or create a formatter instance for a dataclass type.

        Returns cached formatter if available, otherwise creates new instance
        and caches it.

        Info:
            It works with inheritance of dataclasses in the sense of:
            If a custom formatter is not register for a child but for its mother,
            the mother custom formatter will be used for the child. The first match is taken.

        Args:
            dataclass_type: The dataclass type to get formatter for.

        Returns:
            Formatter instance for the dataclass type.
        """
        # Maybe we can parametrize latter the mro inheritance of custom formatters (enable or not)

        if dataclass_type in cls._formatters_cache:
            return cls._formatters_cache[dataclass_type]

        for dt in dataclass_type.mro():
            if (formatter_cls := cls._registry.get(dt)) is not None:
                return cls._formatters_cache.setdefault(dataclass_type, formatter_cls(dataclass_type, *exclude_fields))

        return cls._formatters_cache.setdefault(dataclass_type, DataClassBaseFormatter(dataclass_type, *exclude_fields))
