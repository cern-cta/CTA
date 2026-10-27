# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Final

from .archive_file import ArchiveFile


@dataclass(eq=False)
class TapeFile(ArchiveFile):
    """File stored on tape.

    Attributes:
        vid: Volume id of the tape.
        fseq: File position on the tape.
        block_id: Block id on the tape.
    """

    vid: Final[str]
    fseq: Final[int]
    block_id: int

    def __str__(self) -> str:
        return f"TapeFile({self.archive_id},{self.vid},{self.fseq})"

    def __hash__(self) -> int:
        return hash((self.archive_id, self.vid, self.fseq))

    def __eq__(self, other):
        if not isinstance(other, TapeFile):
            return False
        return self.archive_id == other.archive_id and self.vid == other.vid and self.fseq == other.fseq
