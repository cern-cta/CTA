# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from enum import Enum
from typing import Final, Optional

from typing_extensions import override

from .metadata import Metadata


class LogicalLibraryState(Enum):
    DISABLED = "disabled"
    ENABLED = "enabled"


@dataclass(eq=False)
class LogicalLibrary:
    """Logical library used to group tapes and drives based on location and capabilities.

    Attributes:
        name: Unique name of the logical library.
        physical_library_name: Name of the physical library that backs this logical library.
        is_disabled: Indicates if the library is disabled.
        state_reason: Optional state reason.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    physical_library_name: str
    state: LogicalLibraryState
    state_reason: Optional[str]
    metadata: Metadata

    @override
    def __str__(self) -> str:
        return f"LogicalLibrary({self.name})"

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, LogicalLibrary):
            return False
        return self.name == other.name
