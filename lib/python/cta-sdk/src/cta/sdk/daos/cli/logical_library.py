# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import datetime, timezone
from typing import Iterator, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import LogicalLibraryDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import LogicalLibrary, LogicalLibraryState, Metadata


class LogicalLibraryDaoCli(LogicalLibraryDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "logicallibrary"

    @staticmethod
    def raw_data_to_resource(raw_data: dict) -> LogicalLibrary:
        return LogicalLibrary(
            raw_data["name"],
            physical_library_name=raw_data["physicalLibrary"],
            state=LogicalLibraryState.DISABLED if raw_data["isDisabled"] else LogicalLibraryState.ENABLED,
            state_reason=r if (r := raw_data["disabledReason"]) else None,
            metadata=Metadata(
                raw_data["creationLog"]["username"],
                raw_data["creationLog"]["host"],
                datetime.fromtimestamp(int(raw_data["creationLog"]["time"]), tz=timezone.utc),
                raw_data["lastModificationLog"]["username"],
                raw_data["lastModificationLog"]["host"],
                datetime.fromtimestamp(int(raw_data["lastModificationLog"]["time"]), tz=timezone.utc),
                raw_data["comment"],
            ),
        )

    def get(self, name: str, /) -> LogicalLibrary:
        """Avoid to use multiple times, there is performance issue prefer list()"""
        try:
            raw_ll = next(raw_item for raw_item in self._wrapper.list(self.resource_type) if raw_item["name"] == name)
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"name": name}) from e
        return self.raw_data_to_resource(raw_ll)

    def list(self, *, state: Optional[LogicalLibraryState] = None) -> Iterator[LogicalLibrary]:
        filters = {}
        filters |= {} if state is None else {"disabled": str(state == LogicalLibraryState.DISABLED).lower()}
        yield from (
            self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type, filters=filters)
        )

    def delete(self, name: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"name": name})

    def create(
        self,
        name: str,
        /,
        *,
        physical_library_name: str,
        state: LogicalLibraryState = LogicalLibraryState.DISABLED,
        comment: str = "From python cta-sdk",
    ) -> None:
        data = {
            "physicallibrary": physical_library_name,
            "disabled": str(state == LogicalLibraryState.DISABLED).lower(),
            "comment": comment,
        }

        self._wrapper.create(
            self.resource_type,
            {"name": name},
            data=data,
        )

    def update(
        self,
        name: str,
        /,
        *,
        physical_library_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if physical_library_name is None else {"physicallibrary": physical_library_name}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
            },
            data=data,
        )

    def disable(self, name: str, /, *, reason: str) -> None:
        self._wrapper.update(self.resource_type, {"name": name}, data={"disabled": "true", "disabledreason": reason})

    def enable(self, name: str, /) -> None:
        self._wrapper.update(self.resource_type, {"name": name}, data={"disabled": "false"})
