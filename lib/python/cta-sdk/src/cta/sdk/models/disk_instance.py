# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Final

from typing_extensions import override

from .metadata import Metadata


@dataclass(eq=False)
class DiskInstance:
    """Disk buffer instance where files transit temporary.

    Attributes:
        name: Unique name of the disk instance.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    metadata: Metadata

    @override
    def __str__(self):
        return f"DiskInstance({self.name})"

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, DiskInstance):
            return False
        return self.name == other.name
