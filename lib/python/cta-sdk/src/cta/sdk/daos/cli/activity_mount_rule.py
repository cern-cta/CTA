# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import datetime, timezone
from typing import Iterator, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import ActivityMountRuleDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import ActivityMountRule, Metadata


class ActivityMountRuleDaoCli(ActivityMountRuleDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "activitymountrule"

    @staticmethod
    def raw_data_to_resource(raw_data: dict) -> ActivityMountRule:
        return ActivityMountRule(
            user_name=raw_data["activityMountRule"],
            disk_instance_name=raw_data["diskInstance"],
            regex=raw_data["activityRegex"],
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

    def get(self, name: str, disk_instance: str, regex: str, /) -> ActivityMountRule:
        """Avoid to use multiple times, there is performance issue prefer list()"""

        try:
            raw_amr = next(
                raw_item
                for raw_item in self._wrapper.list(self.resource_type)
                if raw_item["activityMountRule"] == name
                and raw_item["diskInstance"] == disk_instance
                and raw_item["activityRegex"] == regex
            )
        except StopIteration as e:
            raise ResourceNotFoundError(
                self.resource_type, {"activityMountRule": name, "diskInstance": disk_instance, "activityRegex": regex}
            ) from e

        return self.raw_data_to_resource(raw_amr)

    def list(self) -> Iterator[ActivityMountRule]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    def delete(self, name: str, disk_instance: str, regex: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"name": name, "instance": disk_instance, "activityregex": regex})

    def create(
        self,
        name: str,
        disk_instance_name: str,
        regex: str,
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
                "activityregex": regex,
            },
            data={
                "mountpolicy": mount_policy_name,
                "comment": comment,
            },
        )

    def update(
        self,
        name: str,
        disk_instance_name: str,
        regex: str,
        /,
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
                "activityregex": regex,
            },
            data=data,
        )
