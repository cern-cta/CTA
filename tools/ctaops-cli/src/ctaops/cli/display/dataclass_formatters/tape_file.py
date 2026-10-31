# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.models import TapeFile
from ctaops.cli.display.formatters import DataClassBaseFormatter
from typing_extensions import override


class TapeFileFormatter(DataClassBaseFormatter[TapeFile], dataclass_type=TapeFile):
    """Custom Dataclass Formatter for [TapeFile][cta.sdk.models.tape_file.TapeFile]."""

    @override
    def _default_raw_columns_names(self) -> list[str]:
        return [
            "archive_id",
            "vid",
            "size",
            "fseq",
            "block_id",
            "storage_class_name",
            "creation_time",
        ]
