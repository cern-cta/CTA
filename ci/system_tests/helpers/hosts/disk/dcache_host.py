# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import shlex
from functools import cached_property
from pathlib import Path

from typing_extensions import override

from system_tests.helpers.connections.remote_connection import RemoteConnection
from .dcache_client_host import DCACHE_ADMIN_AUTH
from .disk_instance_host import DiskInstanceHost


class DCacheHost(DiskInstanceHost):
    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    @cached_property
    def instance_name(self) -> str:
        return "dcache"

    @cached_property
    def base_dir_path(self) -> Path:
        return Path("/data")

    @cached_property
    def archive_group(self) -> str:
        return "dcacheusers"

    @cached_property
    def storage_class(self) -> str:
        # dCache's default store and storage group are emitted to CTA in this form.
        return "test:tape@cta"

    @cached_property
    def webdav_url(self) -> str:
        return "https://localhost:8083"

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
                f"curl -ksS -u {shlex.quote(DCACHE_ADMIN_AUTH)} -X MKCOL -o /dev/null -w '%{{http_code}}' {url}"
            )
            if status not in ("201", "405"):
                raise RuntimeError(f"Failed to create dCache directory {path}: HTTP status {status}")
            print(f"dCache directory {path}: HTTP {status}")

    @override
    def force_remove_directory(self, directory: Path) -> None:
        raise NotImplementedError
