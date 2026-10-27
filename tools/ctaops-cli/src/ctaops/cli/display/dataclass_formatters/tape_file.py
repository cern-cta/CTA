# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.models import TapeFile

from ..formatters import DataClassBaseFormatter


class TapeFileFormatter(DataClassBaseFormatter, dataclass_type=TapeFile):
    """Custom Dataclass Formatter for [TapeFile][cta.sdk.models.tape_file.TapeFile]."""

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
