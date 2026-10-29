# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.models import Tape

from ..formatters import DataClassBaseFormatter


class TapeFormatter(DataClassBaseFormatter, dataclass_type=Tape):
    """Custom Dataclass Formatter for [Tape][cta.sdk.models.tape.Tape]."""

    def _default_raw_columns_names(self) -> list[str]:
        return [
            "vid",
            "media_type_name",
            "logical_library_name",
            "tape_pool_name",
            "virtual_organisation_name",
            "encryption_key_name",
            "capacity",
            "usage_ratio",
            "statistics.last_fseq",
            "statistics.file_count",
            "statistics.is_full",
            "state",
            "state_reason",
        ]
