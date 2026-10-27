# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Utilities for working with Python dataclass instances and types.

This module provides utilities for dynamically loading data into dataclass instances,
flattening dataclass structures into dictionaries, and analyzing dataclass type hierarchies.

Functions:
    load_data: Recursively load dictionary data into a dataclass instance.
    flatten_dataclass_instance: Convert a dataclass instance to a flattened dict with dot-notation keys.
    flatten_dataclass_class: Analyze a dataclass type and extract all field types with paths.

Protocols:
    DataclassProtocol: Protocol for type hinting dataclass instances.
"""

from dataclasses import fields, is_dataclass
from functools import lru_cache
from typing import Any, ClassVar, Iterator, Optional, Protocol, TypeVar, get_args, get_origin, get_type_hints

from ctaops.utils.typing import normalize_types


class DataclassProtocol(Protocol):
    """
    A protocol for type hinting dataclass instances.

    This protocol is used to indicate that a function expects an instance of a
    dataclass. It ensures that the object has the `__dataclass_fields__`
    attribute, which is present on all dataclass instances.
    """

    __dataclass_fields__: ClassVar[dict]


D = TypeVar("D", bound=DataclassProtocol)


def _iter_dataclass_properties(obj_type: type) -> Iterator[tuple[str, property]]:
    """Yield (name, property) for all properties on a dataclass type, own + inherited.
    A subclass property overrides a same-named property from a parent."""
    seen: dict[str, property] = {}
    for klass in obj_type.__mro__:
        for name, member in vars(klass).items():
            if name not in seen and isinstance(member, property):
                seen[name] = member
    yield from seen.items()


def copy_dataclass(obj: DataclassProtocol) -> dict[str, Any]:
    return {f.name: getattr(obj, f.name) for f in fields(obj)}


def load_data(dataclass_instance: D, data: dict[str, Any]) -> D:
    """
    Recursively loads data from a dictionary into a dataclass instance.

    This function modifies the `dataclass_instance` in place, populating its
    fields from the provided `data` dictionary.
    Notes:
        - If a field in the dataclass is itself a dataclass, `load_data` is
        called recursively for that nested object.
        - Keys in the `data` dictionary that do not correspond to any field in the
        dataclass are ignored, and a warning is logged.

    Args:
        dataclass_instance: The dataclass object to load data into. It will be
                            modified in place.
        data: The dictionary containing the data to load.

    Returns:
        The same dataclass instance, now populated with data.
    """

    for f in fields(dataclass_instance):
        if f.name not in data:
            continue

        if is_dataclass(f.type):
            load_data(getattr(dataclass_instance, f.name), data[f.name])

        elif get_origin(f.type) is list and (args := get_args(f.type)) and is_dataclass((dc_type := args[0])):
            setattr(dataclass_instance, f.name, [dc_type(**dc_map) for dc_map in data[f.name]])  # type: ignore
        else:
            setattr(dataclass_instance, f.name, data[f.name])

    return dataclass_instance


def flatten_dataclass_instance(
    obj: DataclassProtocol,
    max_depth: Optional[int] = None,
    *,
    extra_flatten_fields: tuple[str, ...] = (),
    include_properties: bool = False,
    _prefix: str = "",
) -> dict[str, Any]:
    """
    Recursively flattens a dataclass instance into a dictionary with dot-separated keys.

    Each field of the dataclass becomes a key in the resulting dictionary. Nested dataclasses
    are flattened recursively, with their keys prefixed by the parent field names, separated by dots.

    Args:
        obj (DataclassProtocol): The dataclass instance to flatten. Must be a dataclass at the top level.
        max_depth (Optional[int]): Maximum depth to recurse into nested dataclasses. If None, all levels are flattened.
        _prefix (str): Internal use only. Used to prefix nested keys during recursion.

    Returns:
        dict[str, Any]: A dictionary where keys are field paths in dot notation, and values are
        the corresponding field values. Nested dataclass fields are represented as flattened keys.

    Notes:
        - Non-dataclass values (including lists, dicts, or primitives) are included as-is.
        - This function does not handle dataclass types dynamically—only instances.
        - The function preserves the full nesting structure in the keys using dot notation.
        - `max_depth=0` will return an empty dictionary.
    """
    result = {}

    parent_depth = _prefix.count(".") + bool(_prefix)
    if (
        max_depth is not None
        and parent_depth >= max_depth
        and not any(f.startswith(_prefix) for f in extra_flatten_fields)
    ):
        return {}

    if not is_dataclass(obj) and parent_depth == 0:
        raise TypeError(f"{obj} should be a dataclass instance")

    if not is_dataclass(obj):
        return {_prefix: obj}

    for field in fields(obj):
        value = getattr(obj, field.name)
        key = f"{_prefix}.{field.name}" if _prefix else field.name

        if (
            max_depth is not None
            and parent_depth >= max_depth
            and not any(f.startswith(key) for f in extra_flatten_fields)
        ):
            continue

        if is_dataclass(value):
            result.update(
                flatten_dataclass_instance(
                    value,  # type: ignore
                    max_depth=(max_depth if not any(f == key for f in extra_flatten_fields) else None),
                    extra_flatten_fields=extra_flatten_fields,
                    include_properties=include_properties,
                    _prefix=key,
                )
            )
        else:
            result[key] = value

    if include_properties:
        for name, prop in _iter_dataclass_properties(type(obj)):
            key = f"{_prefix}.{name}" if _prefix else name

            if (
                max_depth is not None
                and parent_depth >= max_depth
                and not any(f.startswith(key) for f in extra_flatten_fields)
            ):
                continue

            assert prop.fget is not None
            value = prop.fget(obj)

            if is_dataclass(value):
                result.update(
                    flatten_dataclass_instance(
                        value,  # type: ignore
                        max_depth=(max_depth if not any(f == key for f in extra_flatten_fields) else None),
                        extra_flatten_fields=extra_flatten_fields,
                        include_properties=include_properties,
                        _prefix=key,
                    )
                )
            else:
                result[key] = value

    return result


@lru_cache
def flatten_dataclass_class(
    obj_type: type,
    max_depth: Optional[int] = None,
    *,
    extra_flatten_fields: tuple[str, ...] = (),
    include_properties: bool = False,
    _prefix: str = "",
) -> list[tuple[str, frozenset[type]]]:
    """
    Recursively flatten a dataclass class into a list of (dot-path, types) tuples,
    privileging dataclass types over other types.

    Notes:
        - If a field type includes one or more dataclass types, recurse into them
        and ignore other type possibilities at this level.
        - If no dataclass types exist, return the normalized set of types for this field.

    Args:
        obj_type: The dataclass type to flatten.
        max_depth: Maximum recursion depth. None = no limit.
        extra_flatten_fields: By pass the max depth limit to add specified field (separate nested structures by '.')
        _prefix: Internal; used to build nested keys.

    Returns:
        List of tuples (key_path: str, types: frozenset[type]).
    """
    parent_depth = _prefix.count(".") + bool(_prefix)

    if (
        max_depth is not None
        and parent_depth >= max_depth
        and not any(f.startswith(_prefix) for f in extra_flatten_fields)
    ):
        return []

    keys: list[tuple[str, frozenset[type]]] = []

    for field in fields(obj_type):
        key = f"{_prefix}.{field.name}" if _prefix else field.name

        if (
            max_depth is not None
            and parent_depth >= max_depth
            and not any(f.startswith(key) for f in extra_flatten_fields)
        ):
            continue

        field_types = normalize_types(field.type)

        # Check for dataclass types
        dataclass_types = {t for t in field_types if is_dataclass(t)}

        if dataclass_types:
            # If any dataclass type exists, recurse and ignore non-dataclass types
            for dc_type in dataclass_types:
                keys.extend(
                    flatten_dataclass_class(
                        dc_type,
                        max_depth=(max_depth if not any(f == key for f in extra_flatten_fields) else None),
                        extra_flatten_fields=extra_flatten_fields,
                        include_properties=include_properties,
                        _prefix=key,
                    )
                )
        else:
            # No dataclass types, include all normalized possibilities
            keys.append((key, field_types))

    if include_properties:
        for name, prop in _iter_dataclass_properties(obj_type):
            key = f"{_prefix}.{name}" if _prefix else name

            if (
                max_depth is not None
                and parent_depth >= max_depth
                and not any(f.startswith(key) for f in extra_flatten_fields)
            ):
                continue

            assert prop.fget is not None
            hints = get_type_hints(prop.fget, include_extras=True)
            prop_types = normalize_types(hints.get("return"))
            dataclass_types = {t for t in prop_types if is_dataclass(t)}

            if dataclass_types:
                for dc_type in dataclass_types:
                    keys.extend(
                        flatten_dataclass_class(
                            dc_type,
                            max_depth=(max_depth if not any(f == key for f in extra_flatten_fields) else None),
                            extra_flatten_fields=extra_flatten_fields,
                            include_properties=include_properties,
                            _prefix=key,
                        )
                    )
            else:
                keys.append((key, prop_types))

    return keys
