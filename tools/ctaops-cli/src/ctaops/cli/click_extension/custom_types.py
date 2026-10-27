import re
from datetime import timedelta

import click
from cta.sdk.models import TapeState


class DurationType(click.ParamType):
    name = "duration"

    _PATTERN = re.compile(
        r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s|m|h|d)$",
        re.IGNORECASE,
    )

    _UNITS = {
        "ms": "milliseconds",
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days",
    }

    def convert(self, value, param, ctx) -> timedelta:
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

    _UNITS = {
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

    def convert(self, value, param, ctx) -> int:
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

    def convert(self, value, param, ctx) -> tuple[TapeState, str]:
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
