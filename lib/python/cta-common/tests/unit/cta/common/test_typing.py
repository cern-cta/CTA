# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Annotated, Any, Final

import pytest
from cta.common.typing import normalize_types


class Unit:
    pass


@pytest.mark.parametrize(
    ("tp", "expected"),
    [
        (Annotated[int | None, "unit"], frozenset({int, type(None)})),
        (Annotated[int, Unit], frozenset({int, Unit})),
        (Final[str | None], frozenset({str, type(None)})),
        (list[dict[str, Any]], frozenset({list})),
    ],
)
def test_nomalize_types(tp: Any, expected: frozenset[type]):
    assert normalize_types(tp) == expected


@pytest.mark.parametrize(
    ("tp", "expected"),
    [
        (Unit, None),
        (list[Unit], None),
        (int | None, int),
        (list[dict[str, Any]], None),
        (dict[str, Any] | None, dict[str, Any]),
    ],
)
def test_unwrap_optional_type(tp: Any, expected: Any):
    from cta.common.typing import unwrap_optional_type

    assert unwrap_optional_type(tp) == expected
