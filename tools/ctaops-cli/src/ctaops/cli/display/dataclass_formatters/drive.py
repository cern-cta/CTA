# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.models import Drive, DriveOperatingState
from rich.text import Text

from ctaops.cli.models.drive import EnrichedDrive

from ..encoders import TableFieldEncoder, encode_table_field
from ..formatters import DataClassBaseFormatter, TableEntry


class DriveFormatter(DataClassBaseFormatter, dataclass_type=Drive):
    """Custom Dataclass Formatter for [Drive][cta.sdk.models.drive.Drive]."""

    def _default_raw_columns_names(self) -> list[str]:
        return [
            "name",
            "host_name",
            "logical_library_name",
            "state",
            "tape_session.mount_type",
            "tape_session.state",
            "tape_session.vid",
            "tape_session.tape_pool_name",
            "tape_session.virtual_organisation_name",
            "tape_session.file_transferred_count",
            "tape_session.speed",
            "state_change_age",
            "metadata.last_refresh_age",
            "state_reason",
        ]

    def table_entry(self, obj: Drive) -> TableEntry:
        custom_field_encoders: dict[str, TableFieldEncoder] = {
            "tape_session.mount_type": lambda value, *col_types: Text(
                encode_table_field(value, *col_types).plain.replace("RING", "").replace("ING", "")
            ),
            "state": lambda value, *col_types: encode_table_field(
                {
                    (DriveOperatingState.UP, DriveOperatingState.DOWN): "STARTING",
                    (DriveOperatingState.DOWN, DriveOperatingState.UP): "STOPPING",
                }.get((obj.desired_state, obj.state), obj.state),
                *col_types,
            ),
        }

        return [
            custom_field_encoders.get(col_name, encode_table_field)(value, *col_types)
            for value, col_name, col_types in zip(
                self._raw_table_entry(obj), self._raw_columns_names(), self._raw_columns_types()
            )
        ]


class EnrichedDriveFormatter(DataClassBaseFormatter, dataclass_type=EnrichedDrive):
    """Custom Dataclass Formatter for [EnrichedDrive][ctaops.cli.models.drive.EnrichedDrive]."""

    def _default_raw_columns_names(self) -> list[str]:
        return [
            "name",
            "host_name",
            "logical_library_name",
            "state",
            "tape_session.mount_type",
            "tape_session.state",
            "tape_session.vid",
            "tape_session.tape_pool_name",
            "tape_session.virtual_organisation_name",
            "tape_session.file_transferred_count",
            "tape_session.speed",
            "state_change_age",
            "metadata.last_refresh_age",
            "serial_number",
            "is_connected",
            "state_reason",
        ]

    def table_entry(self, obj: Drive) -> TableEntry:
        custom_field_encoders: dict[str, TableFieldEncoder] = {
            "tape_session.state": lambda value, *col_types: Text(
                encode_table_field(value, *col_types).plain.replace("RING", "").replace("ING", "")
            ),
            "state": lambda value, *col_types: encode_table_field(
                {
                    (DriveOperatingState.UP, DriveOperatingState.DOWN): "START",
                    (DriveOperatingState.DOWN, DriveOperatingState.UP): "STOP",
                }.get((obj.desired_state, obj.state), obj.state),
                *col_types,
            ),
        }

        return [
            custom_field_encoders.get(col_name, encode_table_field)(value, *col_types)
            for value, col_name, col_types in zip(
                self._raw_table_entry(obj), self._raw_columns_names(), self._raw_columns_types()
            )
        ]
