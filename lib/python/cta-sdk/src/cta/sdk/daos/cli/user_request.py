# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import UserRequestDao
from cta.sdk.models import MountType, UserRequest
from typing_extensions import override


class UserRequestDaoCli(UserRequestDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "queue"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> UserRequest:
        return UserRequest(
            MountType.RETRIEVE if raw_data["mountType"] == "RETRIEVE" else MountType.ARCHIVE,
            tape_pool_name=raw_data["tapepool"],
            vid=r if (r := raw_data["vid"]) else None,
            virtual_organisation_name=raw_data["vo"],
            logical_library_name=raw_data["logicalLibrary"],
            queued_file_count=int(raw_data["queuedFiles"]),
            queued_byte_count=int(raw_data["queuedBytes"]),
            oldest_age=timedelta(seconds=int(raw_data["oldestAge"])),
            priority=int(raw_data["priority"]),
            min_age=timedelta(seconds=int(raw_data["minAge"])),
            mount_count=int(raw_data["curMounts"]),
            transferred_file_count=int(raw_data["curFiles"]),
            transferred_byte_count=int(raw_data["curBytes"]),
            is_sleeping_for_space=raw_data["sleepingForSpace"],
            sleep_start_time=(
                datetime.fromtimestamp(int(raw_data["sleepStartTime"]), tz=timezone.utc)
                if raw_data["sleepStartTime"] not in ("", "0")
                else None
            ),
            scheduler_backend_name=raw_data["schedulerBackendName"],
            tapes_byte_count=int(raw_data["tapesBytes"]),
            tapes_file_count=int(raw_data["tapesFiles"]),
            tapes_capacity=int(raw_data["tapesCapacity"]),
        )

    @override
    def list(self, *, request_type: Optional[MountType] = None) -> Iterator[UserRequest]:
        yield from (
            self.raw_data_to_resource(json.loads(line))
            for line in self._wrapper.run_streaming_command([*self._wrapper.base_command, "showqueues"])
            if (request_type is None or json.loads(line)["mountType"].upper().startswith(request_type.name))
        )
