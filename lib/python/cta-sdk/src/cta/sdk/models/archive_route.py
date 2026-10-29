# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from enum import Enum
from typing import Final

from typing_extensions import override

from .metadata import Metadata


class ArchiveRouteType(Enum):
    """CTA supports two types of archive route.

    Attributes:
        DEFAULT: For user worflow
        REPACK: For repack worflow
    """

    DEFAULT = "default"
    REPACK = "repack"


@dataclass(eq=False)
class ArchiveRoute:
    """ArchiveRoute maps storageClass to a tapepool for archiving workflow.

    Attributes:
        storage_class_name: Name of the storage class.
        copy_number: The route copy_number (MUST be between 1 and storage.number_of_copies).
        type: Type of the route.
        tape_pool_name: Name of the tape pool.
        metadata: Metadata about creation and updates.
    """

    storage_class_name: Final[str]
    copy_number: Final[int]
    type: Final[ArchiveRouteType]
    tape_pool_name: str
    metadata: Metadata

    @override
    def __str__(self):
        return f"ArchiveRoute({self.storage_class_name}, {self.copy_number}, {self.type})"

    @override
    def __hash__(self) -> int:
        return hash((self.storage_class_name, self.copy_number, self.type))

    @override
    def __eq__(self, other: object):
        if not isinstance(other, ArchiveRoute):
            return False
        return (
            self.storage_class_name == other.storage_class_name
            and self.copy_number == other.copy_number
            and self.type == other.type
        )
