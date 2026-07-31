# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


from system_tests.helpers.hosts import DCacheHost, DCacheClientHost
from pathlib import Path

#####################################################################################################################
# Tests
#####################################################################################################################

# TODO: populate catalogue correctly


def test_archive_and_delete(dcache_client: DCacheClientHost, dcache: DCacheHost, test_dir: Path):
    path = test_dir / "group"

    dcache_client.archive_file(dcache.instance_name, path, Path("/etc/group"), wait=True)
    assert dcache_client.is_file_on_tape(dcache.instance_name, path)

    dcache_client.delete_file(dcache.instance_name, path)
    dcache_client.wait_for_file_deletion(path)
