# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from ctaops.cli.display.formatters import DataClassBaseFormatter
from ctaops.cli.models.frontend import Availability, ProbeResult
from typing_extensions import override


class ProbeResultFormatter(DataClassBaseFormatter[ProbeResult], dataclass_type=ProbeResult):
    """Custom Dataclass Formatter for [ProbeResult][ctaops.cli.models.frontend.ProbeResult]."""

    @override
    def _default_raw_columns_names(self) -> list[str]:
        return [
            "backend.service.name",
            "backend.service.port",
            "backend.address",
            "hostname",
            "up",
            "duration",
        ]


class AvailabilityFormatter(DataClassBaseFormatter[Availability], dataclass_type=Availability):
    """Custom Dataclass Formatter for [Availability][ctaops.cli.models.frontend.Availability]."""

    @override
    def _default_raw_columns_names(self) -> list[str]:
        return [
            "service.name",
            "service.port",
            "family",
            "total",
            "up",
            "percent",
        ]
