# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Annotated, Final

from cta.sdk.units import Byte
from typing_extensions import override

from .metadata import Metadata


@dataclass(eq=False)
class VirtualOrganisation:
    """Logical organisation used to seperate clients within CTA.

    Attributes:
        name: Unique name/identifier for the virtual organisation.
        disk_instance_name: Name of the parent disk instance.
        is_repack_vo: Indicate if the VO is used for Repack workflow.
        max_read_drives: Maximum number of drives to use for this VO for reading workflow.
        max_write_drives: Maximum number of drives to use for this VO for archiving workflow.
        max_file_size: Maximum data file size allowed to archive in bytes.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    disk_instance_name: str
    is_repack_vo: bool
    max_read_drives: int
    max_write_drives: int
    max_file_size: Annotated[int, Byte]
    metadata: Metadata

    @override
    def __str__(self) -> str:
        return f"VirtualOrganisation({self.name})"

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, VirtualOrganisation):
            return False
        return self.name == other.name
