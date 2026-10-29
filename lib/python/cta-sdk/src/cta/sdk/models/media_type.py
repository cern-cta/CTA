# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Annotated, Final

from cta.sdk.units import Byte
from typing_extensions import override

from .metadata import Metadata


@dataclass(eq=False)
class MediaType:
    """Used to specify nominal capacity of each media type.

    Attributes:
        name: Unique name of the media type.
        cartridge_type: Cartridge type.
        capacity: Bytes capacity of the cartridge.
        primary_density_code: Define capability of the cartridge.
        secondary_density_code: Define capability of the cartridge.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    cartridge_type: str
    capacity: Annotated[int, Byte]
    primary_density_code: int
    secondary_density_code: int
    metadata: Metadata

    @override
    def __str__(self) -> str:
        return f"MediaType({self.name})"

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, MediaType):
            return False
        return self.name == other.name
