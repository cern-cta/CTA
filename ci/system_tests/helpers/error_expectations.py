# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Optional


class ErrorExpectations:
    """Check logged errors against counted fields or unlimited message allowances."""

    def __init__(self) -> None:
        self._expected: list[tuple[dict[str, str], Optional[int]]] = []
        self._allowed_messages: set[str] = set()

    def expect(self, fields: Mapping[str, str], *, count: Optional[int] = 1) -> None:
        """Expect exact field matches; count=None permits zero or more occurrences."""
        if not fields or (count is not None and count < 1):
            raise ValueError("Provide non-empty fields and a positive count, or count=None")
        self._expected.append((dict(fields), count))

    def allow_message(self, substring: str) -> None:
        """Allow zero or more errors containing this message substring."""
        if not substring:
            raise ValueError("An allowed message substring must not be empty")
        self._allowed_messages.add(substring)

    def verify(self, entries: Sequence[Mapping[str, object]]) -> None:
        """Reject unexpected errors, excess occurrences and missing expected errors."""
        counts = [0] * len(self._expected)
        unexpected: Counter[str] = Counter()
        for entry in entries:
            matching = [
                i
                for i, (fields, _) in enumerate(self._expected)
                if all(entry.get(key) == value for key, value in fields.items())
            ]
            # Counted expectations take precedence; broad allowances cannot hide extra occurrences.
            counted = [i for i in matching if self._expected[i][1] is not None]
            if counted:
                available = [i for i in counted if (limit := self._expected[i][1]) is not None and counts[i] < limit]
                counts[available[0] if available else counted[0]] += 1
            elif matching:
                counts[matching[0]] += 1
            elif not any(pattern in str(entry.get("message", "")) for pattern in self._allowed_messages):
                unexpected[str(dict(entry))] += 1

        problems = [f"Unexpected error ({count} occurrences): {entry}" for entry, count in unexpected.items()]
        for (fields, expected_count), actual_count in zip(self._expected, counts):
            if expected_count is not None and actual_count != expected_count:
                problems.append(f"Expected {expected_count} occurrences, found {actual_count}: {fields}")
        assert not problems, "\n".join(problems)
