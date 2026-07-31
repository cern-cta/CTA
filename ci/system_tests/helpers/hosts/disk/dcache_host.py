# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from functools import cached_property
from pathlib import Path

from typing_extensions import override

from system_tests.helpers.connections.remote_connection import RemoteConnection
from .disk_instance_host import DiskInstanceHost, DiskInstanceImplementation


class DCacheHost(DiskInstanceHost):
    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    @cached_property
    def implementation(self) -> DiskInstanceImplementation:
        return DiskInstanceImplementation.DCACHE

    @cached_property
    def instance_name(self) -> str:
        return "dcache"

    @cached_property
    def base_dir_path(self) -> Path:
        return Path("/data")

    @override
    def mkdir(self, directory: Path, parent: bool = True) -> None:
        raise NotImplementedError

    @override
    def force_remove_directory(self, directory: Path) -> None:
        raise NotImplementedError
