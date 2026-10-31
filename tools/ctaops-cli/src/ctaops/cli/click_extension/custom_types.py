import re
from collections.abc import Callable
from datetime import timedelta
from enum import Enum
from typing import ClassVar, Optional, TypeVar

import click
from cta.sdk.models import TapeState
from typing_extensions import override


class DurationType(click.ParamType):
    name = "duration"

    _PATTERN = re.compile(
        r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s|m|h|d)$",
        re.IGNORECASE,
    )

    _UNITS: ClassVar[dict[str, str]] = {
        "ms": "milliseconds",
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days",
    }

    @override
    def convert(self, value: object, param: Optional[click.Parameter], ctx: Optional[click.Context]) -> timedelta:
        if isinstance(value, timedelta):
            return value

        match = self._PATTERN.fullmatch(str(value).strip())
        if not match:
            self.fail(
                "must be a duration such as '500ms', '10s', '5m', '2h', or '1d'",
                param,
                ctx,
            )

        number = float(match.group("value"))
        unit = str(match.group("unit")).lower()

        return timedelta(**{self._UNITS[unit]: number})


class BytesType(click.ParamType):
    name = "bytes"

    _PATTERN = re.compile(
        r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)?$",
        re.IGNORECASE,
    )

    _UNITS: ClassVar[dict[str, int]] = {
        "b": 1,
        "kb": 10**3,
        "mb": 10**6,
        "gb": 10**9,
        "tb": 10**12,
        "pb": 10**15,
        "kib": 2**10,
        "mib": 2**20,
        "gib": 2**30,
        "tib": 2**40,
        "pib": 2**50,
    }

    @override
    def convert(self, value: object, param: Optional[click.Parameter], ctx: Optional[click.Context]) -> int:
        if isinstance(value, int):
            return value

        match = self._PATTERN.fullmatch(str(value).strip())
        if not match:
            self.fail(
                "must be a size such as '100B', '10MB', '3GB', or '3TiB'",
                param,
                ctx,
            )

        number = float(match.group("value"))
        unit = str(match.group("unit")).lower()

        multiplier = self._UNITS[unit]

        return int(number * multiplier)


class TapeStateType(click.ParamType):
    name = "state[:reason]"

    @override
    def convert(
        self, value: object, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> tuple[TapeState, str]:
        value = str(value).strip()

        state_name, separator, reason = value.partition(":")

        try:
            tape_state = TapeState[state_name.upper()]
        except KeyError:
            valid_states = ", ".join(s.name for s in TapeState)
            self.fail(
                f"invalid state {state_name!r}; choose from: {valid_states}",
                param,
                ctx,
            )

        reason = reason.strip() if separator else ""

        if tape_state is not TapeState.ACTIVE and not reason:
            self.fail(
                f"{tape_state.name} requires a reason (expected STATE:REASON)",
                param,
                ctx,
            )

        return tape_state, reason


E = TypeVar("E", bound=Enum)


def enum_choice(enum_type: type[E]) -> Callable[[click.Context, click.Parameter, Optional[str]], Optional[E]]:
    """Convert a validated Click choice to its enum member."""

    def convert(ctx: click.Context, param: click.Parameter, value: Optional[str]) -> Optional[E]:
        return enum_type[value.upper()] if value is not None else None

    return convert


def uppercase_choice(ctx: click.Context, param: click.Parameter, value: Optional[str]) -> Optional[str]:
    """Normalize a validated case-insensitive Click choice."""
    return value.upper() if value is not None else None
