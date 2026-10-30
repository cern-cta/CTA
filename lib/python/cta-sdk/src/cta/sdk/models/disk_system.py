# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, Final

from cta.sdk.units import Byte
from typing_extensions import override

from .metadata import Metadata


@dataclass(eq=False)
class DiskSystem:
    """Specific directory tree on disk instance.

    Used to configure backpressure at directory level.

    Attributes:
        name: Unique identifier for the disk system.
        disk_instance_name: Name of the parent disk instance.
        disk_instance_space_name: Name of the associated disk instance space.
        file_regex: Regex to map the subdirectory tree.
        desired_min_free_space: Number of bytes to remain free on disk system.
        sleep_time: Time to wait before retrying a mount when there is not enough free space.
        metadata: Creation and modification metadata.
    """

    name: Final[str]
    disk_instance_name: str
    disk_instance_space_name: str
    file_regex: str
    desired_min_free_space: Annotated[int, Byte]
    sleep_time: timedelta
    metadata: Metadata

    @override
    def __str__(self) -> str:
        return f"DiskSystem({self.name})"

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, DiskSystem):
            return False
        return self.name == other.name
