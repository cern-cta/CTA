# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# =========================================================================
#  dCache initialisation
# =========================================================================


from system_tests.helpers.hosts import DCacheHost


def test_dcache_version(dcache: DCacheHost) -> None:
    assert dcache.instance_name == "dcache"
