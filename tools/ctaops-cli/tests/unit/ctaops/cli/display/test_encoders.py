# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the low level display encoders (values, headers and JSON)."""

import json
from datetime import date, datetime, timedelta
from enum import Enum

import pytest
from cta.sdk.units import Byte, BytePerSecond, FreeText, Percentage
from rich.text import Text

from ctaops.cli.display.encoders import (
    CtaSdkJsonEncoder,
    encode_table_field,
    encode_table_header,
    format_bytes,
)


class COLOR(Enum):
    BLUE = "blue"


class TestFormatBytes:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (0, "0B"),
            (1, "1.0B"),
            (999, "999.0B"),
            (1_000, "1.0KB"),
            (1_345_123.3, "1.3MB"),
            (1_000**3, "1.0GB"),
            (1_000**4, "1.0TB"),
            (1_000**5, "1.0PB"),
            (1_000**6, "1.0EB"),
            (1_000**7, "1000.0EB"),  # EB is the biggest unit, bigger values keep piling up on it
        ],
    )
    def test_scales_to_the_biggest_fitting_unit(self, value, expected):
        assert format_bytes(value) == expected

    def test_suffix_is_appended(self):
        assert format_bytes(1_500_000, suffix="/s") == "1.5MB/s"
        assert format_bytes(0, suffix="/s") == "0B/s"


class TestCtaSdkJsonEncoder:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (datetime(2024, 5, 1, 13, 45, 30), int(datetime(2024, 5, 1, 13, 45, 30).timestamp())),
            (date(2024, 5, 1), "2024-05-01"),
            (timedelta(minutes=2), 120),
            (COLOR.BLUE, "BLUE"),
        ],
    )
    def test_encodes_cta_sdk_types(self, value, expected):
        assert json.loads(json.dumps({"field": value}, cls=CtaSdkJsonEncoder)) == {"field": expected}

    def test_unknown_type_still_raises(self):
        with pytest.raises(TypeError):
            json.dumps({"field": object()}, cls=CtaSdkJsonEncoder)


class TestEncodeTableField:
    @pytest.mark.parametrize(
        "obj,types,expected",
        [
            # units drive the formatting of numbers
            (0.121, (float, Percentage), "12.1%"),
            (1_345_123.3, (float, Byte), "1.3MB"),
            (1_345_123, (int, Byte), "1.3MB"),
            (1_500_000, (int, BytePerSecond), "1.5MB/s"),
            # without a unit, ints are printed as-is and floats get one decimal
            (128_930, (int, type(None)), "128930"),
            (3.14159, (float,), "3.1"),
            (128_930, (), "128930"),
            # non numerical types
            (COLOR.BLUE, (COLOR, type(None)), "BLUE"),
            ("some text", (str,), "some text"),
            (True, (bool,), "True"),
            ([1, 2, 3], (list,), "1,2,3"),
            ([COLOR.BLUE, None], (list,), "BLUE,-"),
            ([], (list,), ""),
            (None, (type(None),), "-"),
            (None, (int, type(None)), "-"),  # a missing value stays "-" whatever the column unit is
        ],
    )
    def test_encode(self, obj, types, expected):
        assert encode_table_field(obj, *types).plain == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            (timedelta(seconds=0), "0s"),
            (timedelta(seconds=45), "45s"),
            (timedelta(minutes=5), "5m"),
            (timedelta(hours=2), "2h0m"),
            (timedelta(days=1, hours=2), "1d2h0m"),
        ],
    )
    def test_timedelta(self, value, expected):
        assert encode_table_field(value).plain == expected

    def test_datetime(self):
        assert encode_table_field(datetime(2024, 5, 1, 13, 45, 30)).plain == "2024-05-01 13:45"

    def test_unit_is_picked_among_the_column_types(self):
        """The unit can be at any position, and non-Unit types are ignored."""
        assert encode_table_field(1_000, int, type(None), Byte).plain == "1.0KB"


class TestEncodeTableHeader:
    @pytest.mark.parametrize(
        "col_name,expected",
        [
            ("my.first.column_name", "MY.FIRST.COLUMN"),
            ("column_name", "COLUMN"),
            ("virtual_organisation_name", "VO"),
            ("tape_pool_name", "TAPE_POOL"),
            ("drives_count", "DRIVES"),
            ("file_count", "FILES"),
            ("tape_session.vid", "VID"),
            ("statistics.file_count", "FILES"),
            ("metadata.comment", "COMMENT"),
            ("start_info.time", "START_TIME"),
            ("vid", "VID"),
        ],
    )
    def test_name_is_shortened_and_upper_cased(self, col_name, expected):
        assert encode_table_header(col_name).plain == expected

    def test_free_text_columns_are_left_justified(self):
        assert encode_table_header("metadata.comment", str, FreeText).justify == "left"

    @pytest.mark.parametrize("col_types", [(), (int,), (int, Byte)])
    def test_other_columns_are_right_justified(self, col_types):
        assert encode_table_header("capacity", *col_types).justify == "right"

    def test_returns_a_folding_rich_text(self):
        assert encode_table_header("column_name", int) == Text("COLUMN", justify="right", overflow="fold")
