# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Final

from .metadata import Metadata


@dataclass(eq=False)
class UserMountRule:
    """Used to map a client and mount policy based on client identity.

    Attributes:
        user_name: Client user name on disk system.
        disk_instance_name: Disk instance where the user mounts.
        mount_policy_name: Name of the mount policy to apply.
        metadata: Metadata about creation and updates.
    """

    user_name: Final[str]
    disk_instance_name: Final[str]
    mount_policy_name: str
    metadata: Metadata

    def __str__(self) -> str:
        return f"UserMountRule({self.user_name})"

    def __hash__(self) -> int:
        return hash(self.user_name)

    def __eq__(self, other):
        if not isinstance(other, UserMountRule):
            return False
        return self.user_name == other.user_name and self.disk_instance_name == other.disk_instance_name
