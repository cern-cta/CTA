# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import shlex
from functools import cached_property
from pathlib import Path

from typing_extensions import override

from system_tests.helpers.connections.remote_connection import RemoteConnection
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

    @override
    def mkdir(self, directory: Path, parent: bool = True) -> None:
        if not parent:
            self.exec(f"chimera mkdir {shlex.quote(str(directory))}")
            return

        first_part = 2 if directory.is_absolute() else 1
        for index in range(first_part, len(directory.parts) + 1):
            path = Path(*directory.parts[:index])
            quoted_path = shlex.quote(str(path))
            self.exec(f"chimera ls {quoted_path} >/dev/null 2>&1 || chimera mkdir {quoted_path}")

    @override
    def force_remove_directory(self, directory: Path) -> None:
        raise NotImplementedError
