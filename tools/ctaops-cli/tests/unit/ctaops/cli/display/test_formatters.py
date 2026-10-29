# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the dataclass formatters (table headers, rows, columns and JSON)."""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Annotated, Optional

import pytest
from cta.sdk.units import Byte, FreeText, Percentage
from rich.table import Table
from rich.text import Text

from ctaops.cli.display.formatters import (
    DataClassBaseFormatter,
    DataClassFormatterProvider,
    dataclass_to_rich_table,
)


@dataclass
class Apartment:
    number: int
    floor: int


@dataclass
class Address:
    street: str
    city: str
    zipcode: str
    apartment: Optional[Apartment] = None


@dataclass
class Meta:
    comment: Annotated[str, FreeText]


@dataclass
class Contractor:
    """A dataclass covering every kind of column: scalars, units, nested and metadata."""

    name: str
    rate: Annotated[float, Percentage]
    quota: Annotated[int, Byte]
    hired_at: datetime
    seniority: timedelta
    address: Optional[Address]
    metadata: Meta
    car: Optional[str] = None


def get_formatter(dataclass_type: type, *exclude_fields: str) -> DataClassBaseFormatter:
    """Shortcut on the provider, the entry point to every formatting operation."""
    return DataClassFormatterProvider.get(dataclass_type, *exclude_fields)


def make_contractor(name: str = "Bob Wilson", **overrides) -> Contractor:
    defaults = {
        "rate": 0.25,
        "quota": 1_500,
        "hired_at": datetime(2024, 5, 1, 13, 45, 30),
        "seniority": timedelta(days=2),
        "address": Address("456 Oak Ave", "Portland", "97201", Apartment(1, 1)),
        "metadata": Meta("a comment"),
    }
    defaults.update(overrides)
    return Contractor(name=name, **defaults)  # type: ignore[arg-type]


class TestDefaultFormatter:
    """The default formatter only flattens the first level, plus ``metadata.comment``."""

    def test_headers(self):
        assert get_formatter(Contractor).table_headers() == [
            Text("NAME", justify="right", overflow="fold"),
            Text("RATE", justify="right", overflow="fold"),
            Text("QUOTA", justify="right", overflow="fold"),
            Text("HIRED_AT", justify="right", overflow="fold"),
            Text("SENIORITY", justify="right", overflow="fold"),
            Text("COMMENT", justify="left", overflow="fold"),
            Text("CAR", justify="right", overflow="fold"),
        ]

    def test_entry_is_encoded_with_the_column_units(self):
        """Values are formatted with the unit of their column, missing ones become "-"."""
        assert [t.plain for t in get_formatter(Contractor).table_entry(make_contractor())] == [
            "Bob Wilson",
            "25.0%",
            "1.5KB",
            "2024-05-01 13:45",
            "2d0h0m",
            "a comment",
            "-",
        ]

    def test_entry_keeps_the_header_order_and_length(self):
        formatter = get_formatter(Contractor)
        entry = formatter.table_entry(make_contractor(car="Renault"))
        assert len(entry) == len(formatter.table_headers())
        assert [t.plain for t in entry[-2:]] == ["a comment", "Renault"]

    def test_columns_parameters_defaults(self):
        formatter = get_formatter(Contractor)
        parameters = formatter.table_columns_parameters()
        assert parameters[0] == {"overflow": "fold", "justify": "right"}
        # the FreeText column (metadata.comment) is the only left justified one
        assert parameters[-2] == {"overflow": "fold", "justify": "left"}
        assert len(parameters) == len(formatter.table_headers())

    def test_columns_parameters_are_sized_on_the_given_chunk(self):
        chunk = [make_contractor("Bob"), make_contractor("a-much-longer-name")]
        widths = [p["width"] for p in get_formatter(Contractor).table_columns_parameters(chunk=chunk)]  # type: ignore
        assert widths[0] == len("a-much-longer-name")

    def test_to_json_encodes_nested_and_sdk_types(self):
        assert json.loads(get_formatter(Contractor).to_json(make_contractor())) == {
            "name": "Bob Wilson",
            "rate": 0.25,
            "quota": 1500,
            "hired_at": int(datetime(2024, 5, 1, 13, 45, 30).timestamp()),
            "seniority": 172800,
            "address": {
                "street": "456 Oak Ave",
                "city": "Portland",
                "zipcode": "97201",
                "apartment": {"number": 1, "floor": 1},
            },
            "metadata": {"comment": "a comment"},
            "car": None,
        }


class TestDataClassToRichTable:
    def test_rejects_non_dataclass(self):
        with pytest.raises(ValueError, match="must be a dataclass instance"):
            dataclass_to_rich_table("not a dataclass")  # type: ignore[arg-type]

    def test_builds_a_field_value_table(self):
        table = get_formatter(Contractor).to_table(make_contractor())
        assert isinstance(table, Table)
        assert table.title == "Contractor"
        assert [c.header for c in table.columns] == ["Field", "Value"]
        assert table.row_count == len(vars(make_contractor()))

    def test_nested_dataclasses_become_sub_tables(self):
        table = dataclass_to_rich_table(make_contractor(), title="custom")
        assert table.title == "custom"
        nested = table.columns[1]._cells[5]  # the address field
        assert isinstance(nested, Table)
        assert nested.title == "address"

    def test_list_of_dataclasses_becomes_a_list_table(self):
        @dataclass
        class Fleet:
            cars: list

        table = dataclass_to_rich_table(Fleet([Apartment(1, 1), Apartment(2, 2)]))
        list_table = table.columns[1]._cells[0]
        assert isinstance(list_table, Table)
        assert list_table.title == "cars (List)"
        assert list_table.row_count == 2

    def test_recursion_is_bounded(self):
        @dataclass
        class Node:
            child: Optional["Node"] = None

        deep = Node(Node(Node(Node())))
        rendered = dataclass_to_rich_table(deep, max_depth=2)
        assert isinstance(rendered, Table)
        # at max_depth the recursion stops on a placeholder instead of a sub-table
        assert isinstance(dataclass_to_rich_table(deep, max_depth=0), Text)


class TestFormatterProvider:
    def test_custom_formatter_is_used_for_its_dataclass(self):
        @dataclass
        class Widget:
            name: str
            size: int

        class WidgetFormatter(DataClassBaseFormatter, dataclass_type=Widget):
            def _raw_columns_names(self) -> list:
                return ["size"]

        formatter = get_formatter(Widget)
        assert isinstance(formatter, WidgetFormatter)
        assert formatter.table_headers() == [Text("SIZE", justify="right", overflow="fold")]
        assert [t.plain for t in formatter.table_entry(Widget("w", 3))] == ["3"]

    def test_formatter_instances_are_cached(self):
        @dataclass
        class Gadget:
            name: str

        assert get_formatter(Gadget) is get_formatter(Gadget)

    def test_child_dataclass_inherits_the_parent_formatter(self):
        @dataclass
        class Base:
            name: str
            size: int

        class BaseFormatter(DataClassBaseFormatter, dataclass_type=Base):
            def _raw_columns_names(self) -> list:
                return ["name"]

        @dataclass
        class Child(Base):
            extra: str = "x"

        formatter = get_formatter(Child)
        assert isinstance(formatter, BaseFormatter)
        assert formatter.dataclass_type is Child
        assert [t.plain for t in formatter.table_entry(Child("child", 1))] == ["child"]

    def test_unregistered_dataclass_falls_back_on_the_base_formatter(self):
        @dataclass
        class Plain:
            name: str

        assert type(get_formatter(Plain)) is DataClassBaseFormatter

    def test_overridden_column_names_can_reach_any_depth(self):
        """``_raw_columns_names`` is the source of truth, types follow it whatever the depth."""

        @dataclass
        class Deep:
            name: str
            address: Address
            ratio: Annotated[float, Percentage] = field(default=0.5)

        class DeepFormatter(DataClassBaseFormatter, dataclass_type=Deep):
            def _raw_columns_names(self) -> list:
                return ["address.apartment.number", "ratio", "unknown.column"]

        formatter = get_formatter(Deep)
        deep = Deep("n", Address("s", "c", "z", Apartment(7, 2)))
        # a column name that matches no field of the dataclass is simply dropped
        assert [t.plain for t in formatter.table_headers()] == ["ADDRESS.APARTMENT.NUMBER", "RATIO"]
        assert [t.plain for t in formatter.table_entry(deep)] == ["7", "50.0%"]


class TestExcludedFields:
    """``exclude_fields`` (fed by the ``display.columns`` configuration) drops columns."""

    def test_excluded_fields_are_dropped_from_headers_and_entries(self):
        @dataclass
        class Server:
            name: str
            rate: Annotated[float, Percentage]
            metadata: Meta

        formatter = get_formatter(Server, "rate", "metadata.comment")
        assert [t.plain for t in formatter.table_headers()] == ["NAME"]
        assert [t.plain for t in formatter.table_entry(Server("s", 0.5, Meta("c")))] == ["s"]

    def test_unknown_excluded_field_is_ignored(self):
        @dataclass
        class Router:
            name: str
            port: int

        formatter = get_formatter(Router, "not.a.field")
        assert [t.plain for t in formatter.table_headers()] == ["NAME", "PORT"]

    def test_a_custom_formatter_keeps_its_own_columns(self):
        """``exclude_fields`` only filters the default columns, an override wins over it."""

        @dataclass
        class Switch:
            name: str
            port: int

        class SwitchFormatter(DataClassBaseFormatter, dataclass_type=Switch):
            def _raw_columns_names(self) -> list:
                return ["port"]

        formatter = get_formatter(Switch, "port")
        assert isinstance(formatter, SwitchFormatter)
        assert [t.plain for t in formatter.table_headers()] == ["PORT"]
