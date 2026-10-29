# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import TapePoolDao
from cta.sdk.models import Metadata, TapePool, TapePoolStatistics
from typing_extensions import override


class TapePoolDaoCli(TapePoolDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "tapepool"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> TapePool:
        return TapePool(
            raw_data["name"],
            virtual_organisation_name=raw_data["vo"],
            min_writable_tape_count=int(raw_data["numPartialTapes"]),
            encryption_key_name=r if (r := raw_data["encryptionKeyName"]) else None,
            supply_tape_pool_names=raw_data["supplySource"],
            statistics=TapePoolStatistics(
                tape_count=int(raw_data["numTapes"]),
                file_count=int(raw_data["numPhysicalFiles"]),
                capacity=int(raw_data["capacityBytes"]),
                usage=int(raw_data["dataBytes"]),
            ),
            metadata=Metadata(
                raw_data["created"]["username"],
                raw_data["created"]["host"],
                datetime.fromtimestamp(int(raw_data["created"]["time"]), tz=timezone.utc),
                raw_data["modified"]["username"],
                raw_data["modified"]["host"],
                datetime.fromtimestamp(int(raw_data["modified"]["time"]), tz=timezone.utc),
                raw_data["comment"],
            ),
        )

    @override
    def get(self, name: str, /) -> TapePool:
        return self.raw_data_to_resource(self._wrapper.get(self.resource_type, {"name": name}))

    @override
    def list(
        self,
        /,
        *,
        virtual_organisation_name: Optional[str] = None,
        encrypted: Optional[bool] = None,
    ) -> Iterator[TapePool]:
        filters = {}
        filters |= {} if virtual_organisation_name is None else {"virtualorganisation": virtual_organisation_name}
        filters |= {} if encrypted is None else {"encrypted": str(encrypted).lower()}

        yield from (
            self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type, filters=filters)
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
        virtual_organisation_name: str,
        min_writable_tape_count: int = 1,
        encryption_key_name: Optional[str] = None,
        supply_tape_pool_names: Optional[Iterable[str]] = None,
        comment: str = "From python cta-sdk",
    ) -> None:
        data = {
            "virtualorganisation": virtual_organisation_name,
            "partialtapesnumber": min_writable_tape_count,
            "comment": comment,
        }
        data |= {} if supply_tape_pool_names is None else {"supply": ",".join(supply_tape_pool_names)}
        data |= {} if encryption_key_name is None else {"encryptionkeyname": encryption_key_name}
        self._wrapper.create(
            self.resource_type,
            {
                "name": name,
            },
            data=data,
        )

    @override
    def update(
        self,
        name: str,
        /,
        *,
        virtual_organisation_name: Optional[str] = None,
        min_writable_tape_count: Optional[int] = None,
        supply_tape_pool_names: Optional[Iterable[str]] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if virtual_organisation_name is None else {"virtualorganisation": virtual_organisation_name}
        data |= {} if min_writable_tape_count is None else {"partialtapesnumber": min_writable_tape_count}
        data |= {} if supply_tape_pool_names is None else {"supply": ",".join(supply_tape_pool_names)}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
            },
            data=data,
        )

    @override
    def remove_encryption(self, name: str, /) -> None:
        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
            },
            data={"encryptionkeyname": ""},
        )

    @override
    def set_encryption_key_name(self, name: str, /, *, encryption_key_name: str) -> None:
        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
            },
            data={"encryptionkeyname": encryption_key_name},
        )
