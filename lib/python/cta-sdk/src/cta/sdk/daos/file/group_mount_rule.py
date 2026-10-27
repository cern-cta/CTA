# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterator, Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import GroupMountRuleDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import DiskInstance, GroupMountRule, MountPolicy


class GroupMountRuleDaoFile(GroupMountRuleDao):
    resource_type = GroupMountRule

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def create(
        self,
        group_name: str,
        disk_instance_name: str,
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
        group_mount_rule = GroupMountRule(
            group_name,
            disk_instance_name,
            mount_policy_name=mount_policy_name,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(group_mount_rule)

    def get(self, group_name: str, disk_instance_name: str, /) -> GroupMountRule:
        return self._handler.get(self.resource_type, group_name, disk_instance_name)

    def delete(self, group_name: str, disk_instance_name: str, /) -> None:
        self._handler.delete(self.resource_type, group_name, disk_instance_name)

    def list(self) -> Iterator[GroupMountRule]:
        yield from self._handler.iter(self.resource_type)

    def update(
        self,
        group_name: str,
        disk_instance_name: str,
        /,
        *,
        mount_policy_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        with self._handler.update(self.resource_type, group_name, disk_instance_name) as group_mount_rule:
            if mount_policy_name is not None:
                try:
                    group_mount_rule.mount_policy_name = self._handler.get(MountPolicy, mount_policy_name).name
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e

            group_mount_rule.metadata = self._handler.refresh_metadata(group_mount_rule.metadata, comment=comment)
