# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import PhysicalLibraryDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import Metadata, PhysicalLibrary, PhysicalLibraryState
from typing_extensions import override


class PhysicalLibraryDaoCli(PhysicalLibraryDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "physicallibrary"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> PhysicalLibrary:
        return PhysicalLibrary(
            name=raw_data["name"],
            manufacturer=raw_data["manufacturer"],
            model=raw_data["model"],
            type=raw_data["type"],
            gui_url=raw_data["guiUrl"],
            webcam_url=raw_data["webcamUrl"],
            location=raw_data["location"],
            physical_cartridge_slots=int(raw_data["nbPhysicalCartridgeSlots"]),
            available_cartridge_slots=int(raw_data["nbAvailableCartridgeSlots"]),
            physical_drive_slots=int(raw_data["nbPhysicalDriveSlots"]),
            state=PhysicalLibraryState.DISABLED if raw_data["isDisabled"] else PhysicalLibraryState.ENABLED,
            state_reason=r if (r := raw_data.get("disabledReason")) else None,
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
    def get(self, name: str, /) -> PhysicalLibrary:
        """Avoid to use multiple times, there is performance issue prefer list()."""
        try:
            raw_pl = next(raw_item for raw_item in self._wrapper.list(self.resource_type) if raw_item["name"] == name)
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"name": name}) from e
        return self.raw_data_to_resource(raw_pl)

    @override
    def list(self, *, state: Optional[PhysicalLibraryState] = None) -> Iterator[PhysicalLibrary]:
        yield from (
            self.raw_data_to_resource(raw_item)
            for raw_item in self._wrapper.list(self.resource_type)
            if (state is None or raw_item["isDisabled"] == (state == PhysicalLibraryState.DISABLED))
        )

    @override
    def delete(self, name: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"name": name})

    @override
    def create(
        self,
        name: str,
        /,
        *,
        manufacturer: str,
        model: str,
        physical_cartridge_slots: int,
        physical_drive_slots: int,
        gui_url: Optional[str] = None,
        webcam_url: Optional[str] = None,
        type: Optional[str] = None,
        location: Optional[str] = None,
        available_cartridge_slots: Optional[int] = None,
        comment: str = "From python cta-sdk",
    ) -> None:
        data = {
            "manufacturer": manufacturer,
            "model": model,
            "nbphysicalcartridgeslots": physical_cartridge_slots,
            "nbphysicaldriveslots": physical_drive_slots,
        }
        data |= {} if gui_url is None else {"guiurl": gui_url}
        data |= {} if webcam_url is None else {"webcamurl": webcam_url}
        data |= {} if type is None else {"type": type}
        data |= {} if location is None else {"location": location}
        data |= {} if available_cartridge_slots is None else {"nbavailablecartridgeslots": available_cartridge_slots}
        data |= {"comment": comment}

        self._wrapper.create(
            self.resource_type,
            {"name": name},
            data=data,
        )

    @override
    def update(
        self,
        name: str,
        /,
        *,
        gui_url: Optional[str] = None,
        webcam_url: Optional[str] = None,
        location: Optional[str] = None,
        physical_cartridge_slots: Optional[int] = None,
        available_cartridge_slots: Optional[int] = None,
        physical_drive_slots: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if gui_url is None else {"guiurl": gui_url}
        data |= {} if webcam_url is None else {"webcamurl": webcam_url}
        data |= {} if location is None else {"location": location}
        data |= {} if physical_cartridge_slots is None else {"nbphysicalcartridgeslots": physical_cartridge_slots}
        data |= {} if available_cartridge_slots is None else {"nbavailablecartridgeslots": available_cartridge_slots}
        data |= {} if physical_drive_slots is None else {"nbphysicaldriveslots": physical_drive_slots}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
            },
            data=data,
        )

    @override
    def disable(self, name: str, /, *, reason: str) -> None:
        self._wrapper.update(self.resource_type, {"name": name}, data={"disabled": "true", "disabledreason": reason})

    @override
    def enable(self, name: str, /) -> None:
        self._wrapper.update(self.resource_type, {"name": name}, data={"disabled": "false"})
