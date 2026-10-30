# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, Final

from cta.sdk.units import Byte
from typing_extensions import override

from .metadata import Metadata


@dataclass(eq=False)
class DiskInstanceSpace:
    """Represents a separate partition on disk, used to separate archive and retrieve space.

    Attributes:
        name: Unique name of the disk instance space.
        disk_instance_name: Parent disk instance name.
        free_space_url: URL used to query free space (format: <query protocol>:<name of disk space>).
        refresh_interval: Time interval to renew the cached free_space attribute.
        free_space: Last known number of bytes available since last refresh.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    disk_instance_name: Final[str]
    free_space_url: str
    refresh_interval: timedelta
    free_space: Annotated[int, Byte]
    metadata: Metadata

    @override
    def __str__(self):
        return f"DiskInstanceSpace({self.name}, {self.disk_instance_name})"

    @override
    def __hash__(self) -> int:
        return hash((self.name, self.disk_instance_name))

    @override
    def __eq__(self, other: object):
        if not isinstance(other, DiskInstanceSpace):
            return False
        return self.name == other.name and self.disk_instance_name == other.disk_instance_name
