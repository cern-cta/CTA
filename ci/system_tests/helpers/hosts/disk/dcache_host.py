# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import shlex
from pathlib import Path

from typing_extensions import override

from system_tests.helpers.connections.remote_connection import RemoteConnection

from .dcache_constants import (
    DCACHE_ADMIN_AUTH,
    DCACHE_ARCHIVE_GROUP,
    DCACHE_BASE_DIR,
    DCACHE_INSTANCE_NAME,
    DCACHE_STORAGE_CLASS,
    DCACHE_WEBDAV_PORT,
)
from .disk_instance_host import DiskInstanceHost


class DCacheHost(DiskInstanceHost):
    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    @property
    @override
    def instance_name(self) -> str:
        return DCACHE_INSTANCE_NAME

    @property
    @override
    def base_dir_path(self) -> Path:
        return DCACHE_BASE_DIR

    @property
    @override
    def archive_group(self) -> str:
        return DCACHE_ARCHIVE_GROUP

    @property
    @override
    def storage_class(self) -> str:
        return DCACHE_STORAGE_CLASS

    @property
    @override
    def webdav_url(self) -> str:
        return f"https://localhost:{DCACHE_WEBDAV_PORT}"

    @override
    def mkdir(self, directory: Path, parent: bool = True) -> None:
        paths = [directory]
        if parent:
            first_part = 2 if directory.is_absolute() else 1
            paths = [Path(*directory.parts[:index]) for index in range(first_part, len(directory.parts) + 1)]

        for path in paths:
            remote_path = str(path).lstrip("/")
            url = shlex.quote(f"{self.webdav_url}/{remote_path}")
            print(f"Creating dCache directory {path}")
            status = self.exec_with_output(
                f"curl -ksS --connect-timeout 5 --max-time 10 -u {shlex.quote(DCACHE_ADMIN_AUTH)} "
                f"-X MKCOL -o /dev/null -w '%{{http_code}}' {url}"
            )
            if status not in ("201", "405"):
                raise RuntimeError(f"Failed to create dCache directory {path}: HTTP status {status}")
            print(f"dCache directory {path}: HTTP {status}")

    @override
    def force_remove_directory(self, directory: Path) -> None:
        raise NotImplementedError
