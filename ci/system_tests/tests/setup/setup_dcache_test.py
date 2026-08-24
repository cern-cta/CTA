# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# =========================================================================
#  dCache initialisation
# =========================================================================


from system_tests.helpers.hosts import DCacheHost


def test_dcache_version(dcache: DCacheHost) -> None:
    assert dcache.instance_name == "dcache"


def test_configure_archive_namespace(dcache: DCacheHost) -> None:
    archive_directory = dcache.base_dir_path / "cta"
    chimera = "/opt/dcache/bin/chimera"

    print(f"Configuring {archive_directory} as a tape-backed dCache namespace")
    dcache.exec(f'{chimera} writetag {archive_directory} RetentionPolicy "CUSTODIAL"')
    dcache.exec(f'{chimera} writetag {archive_directory} AccessLatency "NEARLINE"')

    retention_policy = dcache.exec_with_output(f"{chimera} readtag {archive_directory} RetentionPolicy")
    access_latency = dcache.exec_with_output(f"{chimera} readtag {archive_directory} AccessLatency")
    print(f"dCache archive namespace configured: RetentionPolicy={retention_policy}, AccessLatency={access_latency}")

    assert retention_policy == "CUSTODIAL"
    assert access_latency == "NEARLINE"
