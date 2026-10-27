# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Failed request Dao protocol.

This module defines operations for managing failed archive and retrieve requests, which
are requests that encountered errors during processing. Failed requests can be queried
by type, tape pool, volume, and deletion status, with optional error log retrieval.
"""

from typing import Iterator, List, Optional, Protocol

from cta.sdk.models import FailedRequest, FailedRequestSummary, MountType


class FailedRequestDao(Protocol):
    """Data access object protocol for failed request management.

    Provides operations for querying failed archive and retrieve requests, including
    filtering by request type, tape pool, volume, and error log retrieval. Supports
    summary statistics for failed requests.
    """

    def list(
        self,
        *,
        type: Optional[MountType] = None,
        tape_pool_name: Optional[str] = None,
        vid: Optional[str] = None,
        logs: bool = False,
    ) -> Iterator[FailedRequest]:
        """List failed requests with optional filtering and log retrieval.

        Args:
            type: Filter by request type (ARCHIVE or RETRIEVE, optional).
            tape_pool_name: Filter by tape pool name (optional).
            vid: Filter by tape volume ID (optional).
            logs: Include error logs in results (default: False).

        Yields:
            FailedRequest: A failed request matching the criteria.
        """
        ...

    def delete(self, id: str, /) -> None:
        """Delete a failed request by ID.

        Args:
            id: The unique identifier of the failed request to delete.

        Returns:
            None
        """
        ...

    def summary(self) -> List[FailedRequestSummary]:
        """Retrieve a summary of failed requests.

        Returns:
            List[FailedRequestSummary]: Summary statistics for failed requests grouped
                                       by request type and other relevant criteria.
        """
        ...
