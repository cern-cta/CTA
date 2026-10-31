# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Final

from typing_extensions import override

from .metadata import Metadata


@dataclass(eq=False)
class ActivityMountRule:
    """Used to map a client and mount policy based on activity xattr of the file.

    Attributes:
        user_name: Client user name on disk system.
        disk_instance_name: Disk instance where the activity is evaluated.
        regex: The activity matching regex for mapping the file to the mount policy.
        mount_policy_name: Name of the mount policy to apply.
        metadata: Metadata about creation and updates.
    """

    user_name: Final[str]
    disk_instance_name: Final[str]
    regex: Final[str]
    mount_policy_name: str
    metadata: Metadata

    @override
    def __str__(self):
        return f"ActivityMountRule({self.user_name}, {self.disk_instance_name}, {self.regex})"

    @override
    def __hash__(self) -> int:
        return hash((self.user_name, self.disk_instance_name, self.regex))

    @override
    def __eq__(self, other: object):
        if not isinstance(other, ActivityMountRule):
            return False
        return (
            self.user_name == other.user_name
            and self.disk_instance_name == other.disk_instance_name
            and self.regex == other.regex
        )
