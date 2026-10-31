# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.models import TapePool
from ctaops.cli.display.formatters import DataClassBaseFormatter
from typing_extensions import override


class TapepoolFormatter(DataClassBaseFormatter[TapePool], dataclass_type=TapePool):
    """Custom Dataclass Formatter for [TapePool][cta.sdk.models.tape_pool.TapePool]."""

    @override
    def _default_raw_columns_names(self) -> list[str]:
        return [
            "name",
            "virtual_organisation_name",
            "statistics.tape_count",
            "statistics.file_count",
            "statistics.capacity",
            "statistics.usage",
            "statistics.usage_percent",
            "encryption_key_name",
            "min_writable_tape_count",
            "supply_tape_pool_names",
        ]
