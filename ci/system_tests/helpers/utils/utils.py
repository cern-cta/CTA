# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import time
from typing import Callable, Any

from .timeout import Timeout


# We could let this return a boolean, but we get better error messages with individual asserts
def assert_dict_equals(actual: dict, expected: dict, ignore_keys: list[str]):
    ignored = set(ignore_keys)
    for k1, v1 in actual.items():
        assert k1 in ignored or (k1 in expected and expected[k1] == v1)
    for k2, v2 in expected.items():
        assert k2 in ignored or k2 in actual


def wait_for_condition(cond_func: Callable[[], bool], timeout_secs: float = 10, interval_secs: float = 0.5) -> None:
    with Timeout(timeout_secs) as t:
        while not cond_func() and not t.expired:
            time.sleep(interval_secs)
        if t.expired:
            raise TimeoutError(f"Condition failed to change to True within in {timeout_secs} seconds")


def canonicalize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: canonicalize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        canon_items = [canonicalize(v) for v in obj]
        return tuple(sorted(canon_items, key=_sort_key))
    else:
        return obj


def _sort_key(x: Any):
    return json.dumps(x, sort_keys=True, separators=(",", ":"))
