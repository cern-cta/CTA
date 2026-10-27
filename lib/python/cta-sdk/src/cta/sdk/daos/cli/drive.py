# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta
from typing import Iterator, Optional

from cta.sdk.daos.cli._wrapper import FLAG, CtaCliWrapper
from cta.sdk.daos.protocols import DriveDao
from cta.sdk.models import (
    Drive,
    DriveConfig,
    DriveMetadata,
    DriveOperatingState,
    MountType,
    TapeSession,
    TapeSessionState,
)


class DriveDaoCli(DriveDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "drive"

    @staticmethod
    def raw_data_to_resource(raw_data: dict) -> Drive:
        drive = Drive(
            name=raw_data["driveName"],
            logical_library_name=raw_data["logicalLibrary"],
            host_name=raw_data["host"],
            desired_state=DriveOperatingState[raw_data["desiredDriveState"]],
            state=DriveOperatingState.DOWN if raw_data["driveStatus"] == "DOWN" else DriveOperatingState.UP,
            state_reason=raw_data["reason"],
            device_path=raw_data["devFileName"],
            physical_library_name=raw_data["physicalLibrary"],
            physical_library_slot=raw_data["rawLibrarySlot"],
            tape_session=None,
            state_change_age=timedelta(seconds=int(raw_data["driveStatusSince"])),
            config=DriveConfig(
                {f"{item['category']}.{item['key']}": item["value"] for item in raw_data["driveConfig"]}
            ),
            metadata=DriveMetadata(
                last_refresh_age=timedelta(seconds=int(raw_data["timeSinceLastUpdate"])),
                cta_version=raw_data["ctaVersion"],
                scheduler_namespace=raw_data["schedulerBackendName"],
                comment=raw_data["comment"],
            ),
        )

        if raw_data["mountType"] != "NO_MOUNT":
            drive.tape_session = TapeSession(
                id=int(raw_data["sessionId"]),
                vid=raw_data["vid"],
                tape_pool_name=raw_data["tapepool"],
                mount_type=MountType.RETRIEVE if raw_data["mountType"] == "RETRIEVE" else MountType.ARCHIVE,
                file_transferred_count=int(raw_data["filesTransferredInSession"]),
                byte_transferred_count=int(raw_data["bytesTransferredInSession"]),
                virtual_organisation_name=raw_data["vo"],
                state=TapeSessionState[
                    {"DRAININGTODISK": "DRAINING_TO_DISK", "CLEANINGUP": "CLEANING_UP"}.get(
                        raw_data["driveStatus"], raw_data["driveStatus"]
                    )
                ],
                duration=timedelta(seconds=int(raw_data["driveStatusSince"])),
            )

        return drive

    def get(self, name: str, /) -> Drive:
        return self.raw_data_to_resource(self._wrapper.get(self.resource_type, name))

    def list(self, /, *, all: bool = False) -> Iterator[Drive]:
        yield from (
            self.raw_data_to_resource(raw_item)
            for raw_item in self._wrapper.list(self.resource_type, {"all": FLAG} if all else {})
        )

    def delete(self, name: str, /, *, force: bool = False) -> None:
        self._wrapper.delete(self.resource_type, name, {"force": FLAG} if force else {})

    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        data = {}
        data |= {} if comment is None else {"comment": comment}
        self._wrapper.update(self.resource_type, name, data)

    def down(self, name: str, /, *, reason: str, force: bool = False) -> None:
        data = {"reason": reason}
        data |= {"force": FLAG} if force else {}
        self._wrapper.put_down(self.resource_type, name, data)

    def up(self, name: str, /, *, reason: Optional[str] = None) -> None:
        data = {}
        data |= {} if reason is None else {"reason": reason}
        self._wrapper.put_up(self.resource_type, name, data)
