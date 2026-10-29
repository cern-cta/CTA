# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Final

from typing_extensions import override

from .metadata import Metadata


@dataclass(eq=False)
class StorageClass:
    """
    Used to separate archive workflow (file location on tapes) within a virtual organisation.

    Attributes:
        name: Unique name of the storage class.
        virtual_organisation_name: Name of the virtual organisation this storage class belongs to.
        replica_count: Number of file copies on distinct tapes (specify number of archive routes needed for this storage
            class).
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    virtual_organisation_name: str
    replica_count: int
    metadata: Metadata

    @override
    def __str__(self) -> str:
        return f"StorageClass({self.name})"

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, StorageClass):
            return False
        return self.name == other.name
