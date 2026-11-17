# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


# =========================================================================
#  dCache Teardown
# =========================================================================


import pytest

from system_tests.helpers.test_env import TestEnv

pytestmark = pytest.mark.destructive()


def test_cleanup_dcache_directories(env: TestEnv) -> None: ...
