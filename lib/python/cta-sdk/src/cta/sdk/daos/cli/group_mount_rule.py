# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import GroupMountRuleDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import GroupMountRule, Metadata
from typing_extensions import override


class GroupMountRuleDaoCli(GroupMountRuleDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "groupmountrule"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> GroupMountRule:
        return GroupMountRule(
            group_name=raw_data["groupMountRule"],
            disk_instance_name=raw_data["diskInstance"],
            mount_policy_name=raw_data["mountPolicy"],
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
    def get(self, name: str, disk_instance: str, /) -> GroupMountRule:
        """Avoid to use multiple times, there is performance issue prefer list()."""
        try:
            raw_gmr = next(
                raw_item
                for raw_item in self._wrapper.list(self.resource_type)
                if raw_item["groupMountRule"] == name and raw_item["diskInstance"] == disk_instance
            )
        except StopIteration as e:
            raise ResourceNotFoundError(
                self.resource_type, {"groupMountRule": name, "diskInstance": disk_instance}
            ) from e
        return self.raw_data_to_resource(raw_gmr)

    @override
    def list(self) -> Iterator[GroupMountRule]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    @override
    def delete(self, name: str, disk_instance: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"name": name, "instance": disk_instance})

    @override
    def create(
        self,
        name: str,
        disk_instance_name: str,
        /,
        *,
        mount_policy_name: str,
        comment: str = "From python cta-sdk",
    ) -> None:
        self._wrapper.create(
            self.resource_type,
            {
                "name": name,
                "instance": disk_instance_name,
            },
            data={"mountpolicy": mount_policy_name, "comment": comment},
        )

    @override
    def update(
        self,
        name: str,
        disk_instance_name: str,
        /,
        *,
        mount_policy_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if mount_policy_name is None else {"mountpolicy": mount_policy_name}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
                "instance": disk_instance_name,
            },
            data=data,
        )
