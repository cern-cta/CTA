# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta
from itertools import chain
from typing import Iterator, Optional, Union

from cta.sdk.daos.file._handler import CtaFileHandler, as_timedelta
from cta.sdk.daos.protocols import MountPolicyDao
from cta.sdk.exceptions import BackendError
from cta.sdk.models import ActivityMountRule, GroupMountRule, MountPolicy, UserMountRule


class MountPolicyDaoFile(MountPolicyDao):
    resource_type = MountPolicy

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

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
        mount_policy = MountPolicy(
            name,
            archive_priority=archive_priority,
            min_archive_age=as_timedelta(min_archive_age),
            retrieve_priority=retrieve_priority,
            min_retrieve_age=as_timedelta(min_retrieve_age),
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(mount_policy)

    def get(self, name: str, /) -> MountPolicy:
        return self._handler.get(self.resource_type, name)

    def delete(self, name: str, /) -> None:
        for mr in chain(
            self._handler.iter(UserMountRule),
            self._handler.iter(GroupMountRule),
            self._handler.iter(ActivityMountRule),
        ):
            if mr.mount_policy_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {mr} ",
                    2,
                    mr,
                )

        self._handler.delete(self.resource_type, name)

    def list(self) -> Iterator[MountPolicy]:
        yield from self._handler.iter(self.resource_type)

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
        with self._handler.update(self.resource_type, name) as mount_policy:
            if archive_priority is not None:
                mount_policy.archive_priority = archive_priority
            if min_archive_age is not None:
                mount_policy.min_archive_age = as_timedelta(min_archive_age)
            if retrieve_priority is not None:
                mount_policy.retrieve_priority = retrieve_priority
            if min_retrieve_age is not None:
                mount_policy.min_retrieve_age = as_timedelta(min_retrieve_age)

            mount_policy.metadata = self._handler.refresh_metadata(mount_policy.metadata, comment=comment)
