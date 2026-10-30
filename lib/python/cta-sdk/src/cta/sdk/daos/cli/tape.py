# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import FLAG, CtaCliWrapper
from cta.sdk.daos.protocols import TapeDao
from cta.sdk.models import Tape, TapeMetadata, TapeState, TapeStatistics, VerificationStatus
from typing_extensions import override


class TapeDaoCli(TapeDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "tape"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> Tape:
        verification_status = None
        if r := raw_data["verificationStatus"]:
            vs_raw_dict = json.loads(r)
            verification_status = VerificationStatus(
                date=datetime.fromtimestamp(vs_raw_dict["date"], tz=timezone.utc),
                status=vs_raw_dict["status"],
                file_submitted_count=vs_raw_dict["files_submitted"],
                file_verified_count=vs_raw_dict["files_verified"],
                file_failed_count=vs_raw_dict["files_failed"],
            )

        return Tape(
            vid=raw_data["vid"],
            media_type_name=raw_data["mediaType"],
            vendor=raw_data["vendor"],
            purchase_order=raw_data["purchaseOrder"],
            logical_library_name=raw_data["logicalLibrary"],
            physical_library_name=raw_data["physicalLibrary"],
            tape_pool_name=raw_data["tapepool"],
            virtual_organisation_name=raw_data["vo"],
            encryption_key_name=r if (r := raw_data["encryptionKeyName"]) else None,
            capacity=int(raw_data["capacity"]),
            format=raw_data["labelFormat"],
            state=TapeState[raw_data["state"]],
            state_reason=raw_data["stateReason"],
            verification_status=verification_status,
            statistics=TapeStatistics(
                physical_usage=int(raw_data["occupancy"]),
                logical_usage=int(raw_data["masterDataInBytes"]),
                is_dirty=raw_data["dirty"],
                last_fseq=int(raw_data["lastFseq"]),
                is_full=raw_data["full"],
                file_count=int(raw_data["nbMasterFiles"]),
            ),
            metadata=TapeMetadata(
                raw_data["creationLog"]["username"],
                raw_data["creationLog"]["host"],
                datetime.fromtimestamp(int(raw_data["creationLog"]["time"]), tz=timezone.utc),
                raw_data["lastModificationLog"]["username"],
                raw_data["lastModificationLog"]["host"],
                datetime.fromtimestamp(int(raw_data["lastModificationLog"]["time"]), tz=timezone.utc),
                raw_data["comment"],
                label_drive_name=raw_data.get("labelLog", {}).get("drive"),
                label_time=(
                    datetime.fromtimestamp(int(time), tz=timezone.utc)
                    if (time := raw_data.get("labelLog", {}).get("time"))
                    else None
                ),
                last_write_drive_name=raw_data.get("lastWrittenLog", {}).get("drive"),
                last_write_time=(
                    datetime.fromtimestamp(int(time), tz=timezone.utc)
                    if (time := raw_data.get("lastWrittenLog", {}).get("time"))
                    else None
                ),
                last_read_drive_name=raw_data.get("lastReadLog", {}).get("drive"),
                last_read_time=(
                    datetime.fromtimestamp(int(time), tz=timezone.utc)
                    if (time := raw_data.get("lastReadLog", {}).get("time"))
                    else None
                ),
                update_state_time=datetime.fromtimestamp(int(raw_data["stateUpdateTime"]), tz=timezone.utc),
                update_state_info=raw_data["stateModifiedBy"],
                write_mount_count=int(raw_data["writeMountCount"]),
                read_mount_count=int(raw_data["readMountCount"]),
            ),
        )

    @override
    def get(self, vid: str, /) -> Tape:
        return self.raw_data_to_resource(self._wrapper.get("tape", {"vid": vid}))

    @override
    def list(
        self,
        /,
        *,
        media_type_name: Optional[str] = None,
        vendor: Optional[str] = None,
        logical_library_name: Optional[str] = None,
        tape_pool_name: Optional[str] = None,
        virtual_organisation_name: Optional[str] = None,
        full: Optional[bool] = None,
        fxid_file: Optional[str] = None,
        state: Optional[TapeState] = None,
        purchase_order: Optional[str] = None,
        physical_library_name: Optional[str] = None,
        missing_file_copies: bool = False,
    ) -> Iterator[Tape]:
        filters = {}
        filters |= {} if media_type_name is None else {"mediatype": media_type_name}
        filters |= {} if vendor is None else {"vendor": vendor}
        filters |= {} if logical_library_name is None else {"logicallibrary": logical_library_name}
        filters |= {} if tape_pool_name is None else {"tapepool": tape_pool_name}
        filters |= {} if virtual_organisation_name is None else {"virtualorganisation": virtual_organisation_name}
        filters |= {} if full is None else {"full": str(full).lower()}
        filters |= {} if fxid_file is None else {"fxidfile": fxid_file}
        filters |= {} if state is None else {"state": state.name}
        filters |= {} if purchase_order is None else {"purchaseorder": purchase_order}
        filters |= {} if physical_library_name is None else {"physicallibrary": physical_library_name}
        filters |= {"missingfilecopies": FLAG} if missing_file_copies is True else {}
        if not filters:
            filters = {"all": FLAG}

        yield from (
            self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.iter(self.resource_type, filters=filters)
        )

    @override
    def delete(self, vid: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"vid": vid})

    @override
    def reclaim(self, vid: str, /) -> None:
        self._wrapper.reclaim(self.resource_type, {"vid": vid})

    @override
    def create(
        self,
        vid: str,
        /,
        *,
        media_type_name: str,
        vendor: str,
        logical_library_name: str,
        tape_pool_name: str,
        full: bool = False,
        purchase_order: Optional[str] = None,
        comment: str = "From python cta-sdk",
    ) -> None:
        data = {
            "mediatype": media_type_name,
            "vendor": vendor,
            "logicallibrary": logical_library_name,
            "tapepool": tape_pool_name,
            "full": str(full).lower(),
        }
        data |= {} if purchase_order is None else {"purchaseorder": purchase_order}
        data |= {"comment": comment}
        self._wrapper.create(
            self.resource_type,
            {
                "vid": vid,
            },
            data=data,
        )

    @override
    def update(
        self,
        vid: str,
        /,
        *,
        media_type_name: Optional[str] = None,
        vendor: Optional[str] = None,
        logical_library_name: Optional[str] = None,
        tape_pool_name: Optional[str] = None,
        verification_status: Optional[VerificationStatus] = None,
        full: Optional[bool] = None,
        purchase_order: Optional[str] = None,
        dirty: Optional[bool] = None,
        encryption_key_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if media_type_name is None else {"mediatype": media_type_name}
        data |= {} if vendor is None else {"vendor": vendor}
        data |= {} if logical_library_name is None else {"logicallibrary": logical_library_name}
        data |= {} if tape_pool_name is None else {"tapepool": tape_pool_name}
        data |= (
            {}
            if verification_status is None
            else {
                "verificationstatus": json.dumps(
                    {
                        "date": int(verification_status.date.timestamp()),
                        "status": verification_status.status,
                        "files_submitted": verification_status.file_submitted_count,
                        "files_verified": verification_status.file_verified_count,
                        "files_failed": verification_status.file_failed_count,
                    }
                )
            }
        )
        data |= {} if full is None else {"full": str(full).lower()}
        data |= {} if purchase_order is None else {"purchaseorder": purchase_order}
        data |= {} if dirty is None else {"dirtybit": str(dirty).lower()}
        data |= {} if encryption_key_name is None else {"encryptionkeyname": encryption_key_name}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "vid": vid,
            },
            data=data,
        )

    @override
    def change_state(
        self,
        vid: str,
        /,
        *,
        state: TapeState,
        reason: str,
    ) -> None:
        self._wrapper.update(self.resource_type, {"vid": vid}, data={"state": state.name, "reason": reason})
