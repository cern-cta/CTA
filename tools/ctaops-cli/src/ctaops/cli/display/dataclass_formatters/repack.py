# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.models import RepackRequest
from ctaops.cli.display.formatters import DataClassBaseFormatter
from typing_extensions import override


class RepackRequestFormatter(DataClassBaseFormatter[RepackRequest], dataclass_type=RepackRequest):
    """Custom Dataclass Formatter for [RepackRequest][cta.sdk.models.repack_request.RepackRequest]."""

    @override
    def _default_raw_columns_names(self) -> list[str]:
        return [
            "start_info.creation_time",
            "duration",
            "vid",
            "tape_pool_name",
            "start_info.file_count",
            "start_info.byte_count",
            "retrieve_statistics.total_file_count",
            "retrieve_statistics.remaining_file_count",
            "archive_statistics.remaining_file_count",
            "progress",
            "failed_file_count",
            "state",
        ]
