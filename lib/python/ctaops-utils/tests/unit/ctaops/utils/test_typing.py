from typing import Annotated, Any, Final, Optional

import pytest
from ctaops.utils.typing import normalize_types


class Unit:
    pass


@pytest.mark.parametrize(
    ("tp", "expected"),
    [
        (Annotated[Optional[int], "unit"], frozenset({int, type(None)})),
        (Annotated[int, Unit], frozenset({int, Unit})),
        (Final[Optional[str]], frozenset({str, type(None)})),
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
        (Optional[int], int),
        (list[dict[str, Any]], None),
        (Optional[dict[str, Any]], dict[str, Any]),
    ],
)
def test_unwrap_optional_type(tp: Any, expected: Any):
    from ctaops.utils.typing import unwrap_optional_type

    assert unwrap_optional_type(tp) == expected
