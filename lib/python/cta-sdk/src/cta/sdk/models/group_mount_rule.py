# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Final

from .metadata import Metadata


@dataclass(eq=False)
class GroupMountRule:
    """Used to map a group on disk to a mount policy in CTA.

    Attributes:
        group_name: Client user group name on the disk system.
        disk_instance_name: Disk instance where the group is mounted.
        mount_policy_name: Name of the mount policy to apply.
        metadata: Metadata about creation and updates.
    """

    group_name: Final[str]
    disk_instance_name: Final[str]
    mount_policy_name: str
    metadata: Metadata

    def __str__(self) -> str:
        return f"GroupMountRule({self.group_name}, {self.disk_instance_name})"

    def __hash__(self) -> int:
        return hash((self.group_name, self.disk_instance_name))

    def __eq__(self, other):
        if not isinstance(other, GroupMountRule):
            return False
        return self.group_name == other.group_name and self.disk_instance_name == other.disk_instance_name
