# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import time

from .timeout import Timeout


# We could let this return a boolean, but we get better error messages with individual asserts
def assert_dict_equals(actual, expected, ignore_keys):
    ignored = set(ignore_keys)
    for k1, v1 in actual.items():
        assert k1 in ignored or (k1 in expected and expected[k1] == v1)
    for k2, v2 in expected.items():
        assert k2 in ignored or k2 in actual


def wait_for_condition(cond_func, timeout_secs=10, interval_secs=0.5):
    with Timeout(timeout_secs) as t:
        while not cond_func() and not t.expired:
            time.sleep(interval_secs)
        if t.expired:
            raise TimeoutError(f"Condition failed to change to True within in {timeout_secs} seconds")
