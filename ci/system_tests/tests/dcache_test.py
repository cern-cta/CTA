# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


from pathlib import Path

from system_tests.helpers.hosts import DCacheClientHost, DCacheHost

#####################################################################################################################
# Tests
#####################################################################################################################


def test_archive_and_delete(dcache_client: DCacheClientHost, dcache: DCacheHost, test_dir: Path):
    path = test_dir / "group"

    print(f"Archiving test file to {path} using storage class {dcache.storage_class}")
    # A fresh dCache pool holds automatic flushing for five minutes, then scans every minute.
    # Allow seven minutes to cover both delays plus CTA scheduling, tape transfer, and reporting.
    dcache_client.archive_file(dcache.instance_name, path, Path("/etc/group"), wait=True, wait_timeout_secs=420)
    assert dcache_client.is_file_on_tape(dcache.instance_name, path)

    print(f"Archive confirmed; deleting {path}")
    dcache_client.delete_file(dcache.instance_name, path)
    dcache_client.wait_for_file_deletion(dcache.instance_name, path)
