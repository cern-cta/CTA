# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Annotated, Final, Optional

from cta.sdk.units import Byte, Percentage
from typing_extensions import override

from .metadata import Metadata


@dataclass
class TapePoolStatistics:
    """Statistic numbers on the tapepool.

    Attributes:
        tape_count: Number of tapes within the tape pool.
        file_count: Current number of files in the tape pool.
        capacity: Capacity of the tape pool in bytes.
        usage: Current number of bytes in the tape pool.
    """

    tape_count: int
    file_count: int
    capacity: Annotated[int, Byte]
    usage: Annotated[int, Byte]

    @property
    def usage_percent(self) -> Annotated[float, Percentage]:
        """Usage percentage (Percentage between 0 and 1)."""
        return self.usage / (self.capacity or 1)


@dataclass(eq=False)
class TapePool:
    """Used to group tapes logically within the same group.

    Attributes:
        name: Unique name of the tape pool.
        virtual_organisation_name: Name of the virtual organisation this pool belongs to.
        min_writable_tape_count: The minimum number of writable tapes the tapepool should have.
        encryption_key_name: The encryption key name used to encrypt the data on tapes (None means no encryption).
        supply_tape_pool_names: List of supply tapepools used to guarantee min_writable_tape_count.
        statistics: Statistics for the tape pool.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    virtual_organisation_name: str
    min_writable_tape_count: int
    encryption_key_name: Optional[str]
    supply_tape_pool_names: list[str]
    statistics: TapePoolStatistics
    metadata: Metadata

    @override
    def __str__(self) -> str:
        return f"TapePool({self.name})"

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, TapePool):
            return False
        return self.name == other.name
