# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Repack request Dao protocol.

This module defines operations for managing repack requests, which are operations that
consolidate or reorganize data on tape cartridges. Repack requests support various custom
operations and configuration options for tape data optimization.
"""

from typing import Iterator, Literal, Optional, Protocol

from cta.sdk.models import RepackRequest


class RepackRequestDao(Protocol):
    """Data access object protocol for repack request management.

    Provides CRUD operations for repack requests that consolidate or reorganize tape data,
    including custom move and add copies operations.
    """

    def get(self, vid: str, /) -> RepackRequest:
        """Retrieve a specific repack request by volume ID.

        Args:
            vid: The tape volume ID to retrieve repack status for.

        Returns:
            RepackRequest: The repack request object containing operation details.
        """
        ...

    def list(self) -> Iterator[RepackRequest]:
        """List all repack requests.

        Yields:
            RepackRequest: A repack request in the system.
        """
        ...

    def delete(self, vid: str, /) -> None:
        """Delete a repack request.

        Args:
            vid: The tape volume ID of the repack request to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        vid: str,
        /,
        *,
        mount_policy_name: str,
        no_recall: bool = False,
        buffer_url: Optional[str] = None,
        max_files_to_select: Optional[int] = None,
        custom_operation: Optional[Literal["MOVE", "ADDCOPIES"]] = None,
    ) -> None:
        """Create a new repack request.

        Args:
            vid: The tape volume ID to repack.
            mount_policy_name: The mount policy for repack operations.
            no_recall: If True, do not recall archived files during repack (default: False).
            buffer_url: Optional buffer location for temporary data staging.
            max_files_to_select: Maximum number of files to include in repack operation (optional).
            custom_operation: Custom repack operation (MOVE for migration, ADDCOPIES for duplication,
                            optional).

        Returns:
            None
        """
        ...
