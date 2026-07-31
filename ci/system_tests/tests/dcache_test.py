# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

#####################################################################################################################
# Tests
#####################################################################################################################


def test_archive_and_delete(dcache_client, dcache, test_dir):
    path = test_dir / "group"

    dcache_client.archive_file(dcache.instance_name, path, "/etc/group", wait=True)
    assert dcache_client.is_file_on_tape(dcache.instance_name, path)

    dcache_client.delete_file(dcache.instance_name, path)
    dcache_client.wait_for_file_deletion(path)
