# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterator, Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import ActivityMountRuleDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import ActivityMountRule, DiskInstance, MountPolicy


class ActivityMountRuleDaoFile(ActivityMountRuleDao):
    resource_type = ActivityMountRule

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def create(
        self,
        user_name: str,
        disk_instance_name: str,
        regex: str,
        /,
        *,
        mount_policy_name: str,
        comment: str = "From python cta-sdk",
    ) -> None:
        try:
            self._handler.get(DiskInstance, disk_instance_name)
            self._handler.get(MountPolicy, mount_policy_name)
        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e
        activity_mount_rule = ActivityMountRule(
            user_name,
            disk_instance_name,
            regex,
            mount_policy_name=mount_policy_name,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(activity_mount_rule)

    def get(self, user_name: str, disk_instance_name: str, regex: str, /) -> ActivityMountRule:
        return self._handler.get(self.resource_type, user_name, disk_instance_name, regex)

    def delete(self, user_name: str, disk_instance_name: str, regex: str, /) -> None:
        self._handler.delete(self.resource_type, user_name, disk_instance_name, regex)

    def list(self) -> Iterator[ActivityMountRule]:
        yield from self._handler.iter(self.resource_type)

    def update(
        self,
        user_name: str,
        disk_instance_name: str,
        regex: str,
        /,
        mount_policy_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        with self._handler.update(self.resource_type, user_name, disk_instance_name, regex) as activity_mount_rule:
            if mount_policy_name is not None:
                try:
                    activity_mount_rule.mount_policy_name = self._handler.get(MountPolicy, mount_policy_name).name
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e

            activity_mount_rule.metadata = self._handler.refresh_metadata(activity_mount_rule.metadata, comment=comment)
