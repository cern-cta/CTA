# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check EOS immutable-file open modes from a pod with XRootD and Kerberos credentials."""

import argparse

from XRootD import client  # type: ignore[reportMissingImports]
from XRootD.client.flags import OpenFlags  # type: ignore[reportMissingImports]

# XRootD protocol error numbers, rather than local errno values.
NOT_AUTHORIZED = 3010
NOT_FOUND = 3011


def check_open(file_url: str, flags: int, contents: bytes = b"") -> None:
    """Check a successful create/write or an explicitly forbidden modification."""
    file = client.File()
    # Match the original test's owner-read-only creation mode (XrdCl::Access::UR).
    status, _ = file.open(file_url, flags, 0o400 if contents else 0)
    try:
        if contents:
            assert status.ok, f"Open with flags {flags} failed: {status}"
            status, _ = file.write(contents, 0)
            assert status.ok, f"Write with flags {flags} failed: {status}"
        else:
            assert not status.ok, f"Open with flags {flags} unexpectedly allowed modification"
            assert status.errno == NOT_AUTHORIZED, f"Expected kXR_NotAuthorized, got: {status}"
    finally:
        # Close even an unexpectedly successful modification attempt before cleanup.
        if file.is_open():
            status, _ = file.close()
            assert status.ok, f"Close with flags {flags} failed: {status}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file_url", help="Unique root:// URL reserved for this test")
    args = parser.parse_args()
    url = client.URL(args.file_url)
    filesystem = client.FileSystem(f"root://{url.hostid}")

    try:
        check_open(args.file_url, OpenFlags.NEW, b"CONTENTS1")
        check_open(args.file_url, OpenFlags.DELETE | OpenFlags.WRITE, b"CONTENTS2")
        check_open(args.file_url, OpenFlags.WRITE)
        check_open(args.file_url, OpenFlags.UPDATE)
    finally:
        status, _ = filesystem.rm(url.path)
        assert status.ok or status.errno == NOT_FOUND, f"Failed to remove test file: {status}"


if __name__ == "__main__":
    main()
