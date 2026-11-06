# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.cli.display.formatters import DataClassBaseFormatter
from cta.common.dataclasses import flatten_dataclass_class
from cta.tapeserver.sdk.repositories.protocols.drive import LocatedNamedDrive
from cta.tapeserver.sdk.repositories.protocols.library import NamedLibrary
from typing_extensions import override


class LocatedNamedDriveFormatter(DataClassBaseFormatter[LocatedNamedDrive], dataclass_type=LocatedNamedDrive):
    """Custom Dataclass Formatter for [Drive][cta.sdk.models.drive.Drive]."""

    @override
    def _default_raw_columns_names(self) -> list[str]:
        return [
            name
            for name, _ in flatten_dataclass_class(
                self.dataclass_type,
                max_depth=None,
                include_properties=True,
            )
        ]


class NamedLibraryFormatter(DataClassBaseFormatter[NamedLibrary], dataclass_type=NamedLibrary):
    """Custom Dataclass Formatter for [Drive][cta.sdk.models.drive.Drive]."""

    @override
    def _default_raw_columns_names(self) -> list[str]:
        return [
            name
            for name, _ in flatten_dataclass_class(
                self.dataclass_type,
                max_depth=None,
                include_properties=True,
            )
        ]
