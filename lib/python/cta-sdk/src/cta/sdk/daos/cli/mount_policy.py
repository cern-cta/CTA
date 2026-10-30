# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import MountPolicyDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import Metadata, MountPolicy
from typing_extensions import override


class MountPolicyDaoCli(MountPolicyDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "mountpolicy"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> MountPolicy:
        return MountPolicy(
            raw_data["name"],
            archive_priority=int(raw_data["archivePriority"]),
            min_archive_age=timedelta(seconds=int(raw_data["archiveMinRequestAge"])),
            retrieve_priority=int(raw_data["retrievePriority"]),
            min_retrieve_age=timedelta(seconds=int(raw_data["retrieveMinRequestAge"])),
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

    @override
    def get(self, name: str, /) -> MountPolicy:
        """Avoid to use multiple times, there is performance issue prefer list()."""
        try:
            raw_mp = next(raw_item for raw_item in self._wrapper.list(self.resource_type) if raw_item["name"] == name)
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"name": name}) from e
        return self.raw_data_to_resource(raw_mp)

    @override
    def list(self) -> Iterator[MountPolicy]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    @override
    def delete(self, name: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"name": name})

    @override
    def create(
        self,
        name: str,
        /,
        *,
        archive_priority: int = 250,
        min_archive_age: Union[timedelta, int] = 14_400,
        retrieve_priority: int = 150,
        min_retrieve_age: Union[timedelta, int] = 14_400,
        comment: str = "From python cta-sdk",
    ) -> None:
        self._wrapper.create(
            self.resource_type,
            {"name": name},
            data={
                "archivepriority": archive_priority,
                "minarchiverequestage": (
                    int(min_archive_age.total_seconds()) if isinstance(min_archive_age, timedelta) else min_archive_age
                ),
                "retrievepriority": retrieve_priority,
                "minretrieverequestage": (
                    int(min_retrieve_age.total_seconds())
                    if isinstance(min_retrieve_age, timedelta)
                    else min_retrieve_age
                ),
                "comment": comment,
            },
        )

    @override
    def update(
        self,
        name: str,
        /,
        *,
        archive_priority: Optional[int] = None,
        min_archive_age: Union[timedelta, int, None] = None,
        retrieve_priority: Optional[int] = None,
        min_retrieve_age: Union[timedelta, int, None] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if archive_priority is None else {"archivepriority": archive_priority}
        data |= (
            {}
            if min_archive_age is None
            else {
                "minarchiverequestage": (
                    int(min_archive_age.total_seconds()) if isinstance(min_archive_age, timedelta) else min_archive_age
                )
            }
        )
        data |= {} if retrieve_priority is None else {"retrievepriority": retrieve_priority}
        data |= (
            {}
            if min_retrieve_age is None
            else {
                "minretrieverequestage": (
                    int(min_retrieve_age.total_seconds())
                    if isinstance(min_retrieve_age, timedelta)
                    else min_retrieve_age
                )
            }
        )
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
            },
            data=data,
        )
