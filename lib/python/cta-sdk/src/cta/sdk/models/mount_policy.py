# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from typing_extensions import override

from .metadata import Metadata


@dataclass(eq=False)
class MountPolicy:
    """Used by the scheduler to choose priority of mounts.

    Attributes:
        name: Unique name of the mount policy.
        archive_priority: Priority number for archive requests.
        min_archive_age: Minimum duration for archive request to be taken by the scheduler.
        retrieve_priority: Priority number for retrieve requests.
        min_retrieve_age: Minimum duration for retrieve request to be taken by the scheduler.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    archive_priority: int
    min_archive_age: timedelta
    retrieve_priority: int
    min_retrieve_age: timedelta
    metadata: Metadata

    @override
    def __str__(self) -> str:
        return f"MountPolicy({self.name})"

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, MountPolicy):
            return False
        return self.name == other.name
