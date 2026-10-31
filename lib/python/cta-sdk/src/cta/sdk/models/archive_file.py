# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Final

from cta.sdk.units import Byte
from typing_extensions import override


@dataclass(frozen=True)
class Checksum:
    """Checksum information for a stored file.

    Attributes:
        algo_name: Name of the algo_name (e.g. ADLER32).
        value: Hex representation of the checksum.
    """

    algo_name: str
    value: str


@dataclass(eq=False)
class DiskFile:
    """Representation of a file on a disk instance.

    Attributes:
        id: Disk file id on the disk instance.
        disk_instance_name: Disk instance name.
        uid: Owner user id of the file on the disk instance.
        gid: Owner group id of the file on the disk instance.
        path: Path of the file on the disk instance.
    """

    id: Final[str]
    disk_instance_name: Final[str]
    uid: int
    gid: int
    path: str

    @override
    def __str__(self) -> str:
        return f"DiskFile({self.id},{self.disk_instance_name})"

    @override
    def __hash__(self) -> int:
        return hash((self.id, self.disk_instance_name))

    @override
    def __eq__(self, other: object):
        if not isinstance(other, DiskFile):
            return False
        return self.id == other.id and self.disk_instance_name == other.disk_instance_name


@dataclass(eq=False)
class ArchiveFile:
    """Representation of an archived file.

    Attributes:
        archive_id: Unique id in CTA which identifies the disk file.
        storage_class_name: Name of the storage class.
        creation_time: Archival time when the first copy of the file was written.
        size: Number of bytes of the file.
        checksum: Checksum information.
        disk_file: Disk file metadata.
    """

    archive_id: Final[int]
    storage_class_name: str
    creation_time: datetime
    size: Annotated[int, Byte]
    checksum: Checksum
    disk_file: DiskFile

    @override
    def __str__(self) -> str:
        return f"ArchiveFile({self.archive_id})"

    @override
    def __hash__(self) -> int:
        return hash(self.archive_id)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, ArchiveFile):
            return False
        return self.archive_id == other.archive_id
