# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from enum import Enum


class MountType(Enum):
    """The type of the mount

    Attributes:
        ARCHIVE: Data is being written to tape
        RETRIEVE: Data is being written to disk

    """

    ARCHIVE = "archive"
    RETRIEVE = "retrieve"
