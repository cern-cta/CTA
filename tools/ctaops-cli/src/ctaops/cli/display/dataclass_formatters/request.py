# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.models import UserRequestSummary

from ..encoders import encode_table_header
from ..formatters import DataClassBaseFormatter, TableHeaders


class UserRequestSummaryFormatter(DataClassBaseFormatter[UserRequestSummary], dataclass_type=UserRequestSummary):
    """Custom Dataclass Formatter for [UserRequestSummary][cta.sdk.models.user_request.UserRequestSummary]."""

    def _default_raw_columns_names(
        self,
    ) -> list[str]:
        return [
            "group_value",
            "mount_count",
            "queued_byte_count",
            "queued_file_count",
            "transferred_byte_count",
            "transferred_file_count",
            "min_age",
            "oldest_age",
            "highest_priority",
            "tapes_count",
        ]

    def table_headers(self) -> TableHeaders:
        """Get Rich-formatted column definitions for table display.

        Returns:
            List of tuples (column_name, formatting_options_dict).
        """

        return [encode_table_header(self.dataclass_type.group_key, str)] + super().table_headers()[1:]
