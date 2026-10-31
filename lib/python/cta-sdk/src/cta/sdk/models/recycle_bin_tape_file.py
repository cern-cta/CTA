# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from datetime import datetime

from typing_extensions import override

from .tape_file import TapeFile


@dataclass(eq=False)
class RecycleBinTapeFile(TapeFile):
    """Represent a file in the recycle bin.

    Attributes:
        deletion_time: Time when the file was deleted
        deletion_reason: Reason for deletion
        virtual_organisation_name: Name of the virtual organisation
    """

    deletion_time: datetime
    deletion_reason: str
    virtual_organisation_name: str

    @override
    def __str__(self) -> str:
        return f"RecycleBinFile({self.archive_id},{self.vid},{self.fseq})"
