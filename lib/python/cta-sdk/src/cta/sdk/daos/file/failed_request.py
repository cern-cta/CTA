# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import replace
from typing import Iterator, List, Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import FailedRequestDao
from cta.sdk.models import FailedRequest, FailedRequestSummary, MountType


class FailedRequestDaoFile(FailedRequestDao):
    """File implementation of the failed request Dao.

    Note:
        `FailedRequestDao` has no `create`: failed requests are produced by the scheduler.
        Seed them through `CtaFileHandler.create`.
    """

    resource_type = FailedRequest

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def list(
        self,
        *,
        type: Optional[MountType] = None,
        tape_pool_name: Optional[str] = None,
        vid: Optional[str] = None,
        logs: bool = False,
    ) -> Iterator[FailedRequest]:
        for failed_request in self._handler.iter(self.resource_type):
            if type is not None and failed_request.mount_type != type:
                continue
            if tape_pool_name is not None and failed_request.tape_pool_name != tape_pool_name:
                continue
            if vid is not None and failed_request.tape_file.vid != vid:
                continue

            # The CLI only returns failure logs when asked for them.
            yield failed_request if logs else replace(failed_request, logs="")

    def delete(self, id: str, /) -> None:
        self._handler.delete(self.resource_type, id)

    def summary(self) -> List[FailedRequestSummary]:
        # `FailedRequest` carries no scheduler backend name, so unlike the CLI backend the
        # summary can only group by mount type and leaves scheduler_backend_name empty.
        summaries: dict[MountType, FailedRequestSummary] = {}

        for failed_request in self._handler.iter(self.resource_type):
            summary = summaries.setdefault(
                failed_request.mount_type,
                FailedRequestSummary(
                    mount_type=failed_request.mount_type,
                    total_file_count=0,
                    total_byte_count=0,
                    scheduler_backend_name="",
                ),
            )
            summary.total_file_count += 1
            summary.total_byte_count += failed_request.tape_file.size

        return list(summaries.values())
