# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Type annotation utilities for working with Python type hints.

This module provides utilities for normalizing and manipulating Python type annotations.
It includes functionality to extract concrete types from complex type hints including
unions, optionals, literals, and annotated types.

Functions:
    normalize_types: Recursively normalize a type annotation into all possible
                     instantiable concrete types.
"""

import types
from functools import lru_cache
from inspect import isclass
from typing import Annotated, Any, ClassVar, Final, Literal, Optional, Union, get_args, get_origin

TRANSPARENT_TYPES = {Annotated, Final, ClassVar}


def unwrap_optional_type(tp: Any) -> Optional[type]:
    """Return the inner type of an Optional type, or None if not Optional."""
    origin = get_origin(tp)

    # Handle both `Union`` and the new `UnionType` (PEP 604) for Python 3.10+
    if origin is Union or (hasattr(types, "UnionType") and origin is types.UnionType):
        return next((t for t in get_args(tp) if t is not type(None)), None)
    return None


@lru_cache(maxsize=1024)
def normalize_types(tp: Any) -> frozenset[type]:
    """Recursively normalize a Python type annotation into instantiable concrete types.

    This function extracts all possible concrete types from a type annotation, handling:
    - Union and Optional: Extracts all member types
    - Annotated: Extracts the base type and any type metadata (ignores non-type metadata)
    - Final and ClassVar: Extracts the wrapped type
    - Literal: Extracts the runtime type of each literal value
    - Generic types: Extracts the origin type
    - Class types: Returns the class itself as a concrete type

    Args:
        tp: A Python type annotation (can include complex nested types).

    Returns:
        frozenset[type]: A frozenset containing all concrete types that can be
                        instantiated from the input type annotation. Returns an
                        empty frozenset if no concrete types can be extracted.

    Example:
        ```python
        normalize_types(int | str)
        #output: frozenset({<class 'int'>, <class 'str'>})
        normalize_types(Optional[int])
        # output: frozenset({<class 'int'>, <class 'NoneType'>})
        normalize_types(Literal[1, 2])
        # output: frozenset({<class 'int'>})
        ```
    """
    origin = get_origin(tp)

    if origin in TRANSPARENT_TYPES:
        base, *meta = get_args(tp)
        out = set(normalize_types(base))
        if origin is Annotated:
            out.update(m for m in meta if isclass(m))
        return frozenset(out)

    if origin is Union:
        out = set()
        for arg in get_args(tp):
            out.update(normalize_types(arg))
        return frozenset(out)

    if origin is Literal:
        out = {type(lit) for lit in get_args(tp)}
        return frozenset(out)

    if isclass(origin):
        return frozenset({origin})

    if isclass(tp):
        return frozenset({tp})

    return frozenset({})
