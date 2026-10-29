# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Final

from .metadata import Metadata


@dataclass(eq=False)
class AdminUser:
    """Administrator user of CTA, allowed to run Admin commands.

    Attributes:
        name: The administrator's user name.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    metadata: Metadata

    def __str__(self):
        return f"AdminUser({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, AdminUser):
            return False
        return self.name == other.name
