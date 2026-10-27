# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from ctaops.cli.models.frontend import Availability, ProbeResult

from ..formatters import DataClassBaseFormatter


class ProbeResultFormatter(DataClassBaseFormatter, dataclass_type=ProbeResult):
    """Custom Dataclass Formatter for [ProbeResult][ctaops.cli.models.frontend.ProbeResult]."""

    def _default_raw_columns_names(self) -> list[str]:
        return [
            "backend.service.name",
            "backend.service.port",
            "backend.address",
            "hostname",
            "up",
            "duration",
        ]


class AvailabilityFormatter(DataClassBaseFormatter, dataclass_type=Availability):
    """Custom Dataclass Formatter for [Availability][ctaops.cli.models.frontend.Availability]."""

    def _default_raw_columns_names(self) -> list[str]:
        return [
            "service.name",
            "service.port",
            "family",
            "total",
            "up",
            "percent",
        ]
