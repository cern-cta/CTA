# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""UserRequest Dao protocol.

This module defines operations for managing scheduler requests (archive and retrieve operations),
which represent jobs queued for execution by the CTA scheduler. Provides methods to list and
summarize requests with support for grouping by various attributes.
"""

import builtins
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from itertools import groupby
from typing import Literal, Optional

from cta.sdk.models import (
    MountType,
    UserRequest,
    UserRequestSummary,
)

GroupBy = Literal["logical_library_name", "tape_pool_name", "virtual_organisation_name"]

OrderBy = Literal[
    "min_age",
    "oldest_age",
    "queued_file_count",
    "queued_byte_count",
    "mount_count",
    "transferred_file_count",
    "transferred_byte_count",
    "tapes_count",
    "tapes_file_count",
    "tapes_byte_count",
    "tapes_capacity",
    "highest_priority",
]


class UserRequestDao(ABC):
    """Data access object protocol for scheduler request management.

    Provides operations for querying archive and retrieve requests queued in the scheduler,
    including detailed request information and summary statistics with grouping support.

    Note:
        This class uses ``ABC`` rather than ``Protocol`` because ``summary()`` provides a
        concrete static implementation shared by all implementations. The aggregation logic
        operates entirely on already-fetched data and has no reason to vary between backends.
    """

    @abstractmethod
    def list(self, *, request_type: Optional[MountType] = None) -> Iterator[UserRequest]:
        """List scheduler requests with optional type filtering.

        Args:
            request_type: Filter by request type (ARCHIVE or RETRIEVE, optional).

        Yields:
            UserRequest: Request objects matching the specified criteria.
        """
        ...

    @staticmethod
    def summary(
        user_requests: Sequence[UserRequest],
        group_by: GroupBy,
        order_by: OrderBy = "queued_byte_count",
    ) -> builtins.list[UserRequestSummary]:
        """Generate a summary of requests grouped by specified criteria.

        This is a static utility method that aggregates request statistics and provides
        summary information grouped by the specified attribute. All requests must be of the
        same type (ARCHIVE or RETRIEVE).

        Args:
            user_requests: A list of requests to summarize (all of same type).
            group_by: The attribute to group by (logical_library_name, tape_pool_name,
                     or virtual_organisation_name).

        Returns:
            List[UserRequestSummary]: A list of UserRequestSummary objects containing aggregated
                                 statistics for each group.

        Raises:
            ValueError: If requests in the user_requests list have mixed mount types (both ARCHIVE
                       and RETRIEVE).
        """
        if not user_requests:
            return []

        first_request = user_requests[0]

        if any(first_request.mount_type != q.mount_type for q in user_requests):
            raise ValueError("Only treat same type of user_requests, RETRIEVE or ARCHIVE")

        grouped_requests: list[UserRequestSummary] = []
        cls = UserRequestSummary.group_by(group_by)

        for k, elts in groupby(
            sorted(user_requests, key=lambda q: getattr(q, group_by)), key=lambda q: getattr(q, group_by)
        ):
            tapes = set()
            group_request = cls(group_value=k)

            for e in elts:
                group_request.queued_file_count += e.queued_file_count
                group_request.queued_byte_count += e.queued_byte_count
                group_request.mount_count += e.mount_count
                group_request.transferred_file_count += e.transferred_file_count
                group_request.transferred_byte_count += e.transferred_byte_count
                group_request.tapes_file_count += e.tapes_file_count
                group_request.tapes_byte_count += e.tapes_byte_count
                group_request.tapes_capacity += e.tapes_capacity
                group_request.min_age = min(e.min_age, group_request.min_age)
                group_request.oldest_age = max(e.oldest_age, group_request.oldest_age)
                group_request.highest_priority = max(e.priority, group_request.highest_priority)

                if first_request.mount_type == MountType.RETRIEVE:
                    tapes.add(e.vid)

            if first_request.mount_type == MountType.RETRIEVE:
                group_request.tapes_count = len(tapes)

            grouped_requests.append(group_request)

        return sorted(grouped_requests, key=lambda gr: getattr(gr, order_by))
