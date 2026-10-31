# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterable, Iterator
from itertools import chain
from typing import Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import TapePoolDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import ArchiveRoute, Tape, TapePool, TapePoolStatistics, VirtualOrganisation
from typing_extensions import override


class TapePoolDaoFile(TapePoolDao):
    resource_type = TapePool

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

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
        try:
            self._handler.get(VirtualOrganisation, virtual_organisation_name)
            if supply_tape_pool_names is not None:
                [
                    self._handler.get(TapePool, tp_name) for tp_name in supply_tape_pool_names
                ]  # use list just to consume iterator
        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e
        tape_pool = TapePool(
            name,
            virtual_organisation_name=virtual_organisation_name,
            min_writable_tape_count=min_writable_tape_count,
            encryption_key_name=encryption_key_name,
            supply_tape_pool_names=list(supply_tape_pool_names or []),
            statistics=TapePoolStatistics(tape_count=0, file_count=0, capacity=0, usage=0),
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(tape_pool)

    @override
    def get(self, name: str, /) -> TapePool:
        return self._handler.get(self.resource_type, name)

    @override
    def delete(self, name: str, /) -> None:
        for obj in chain(
            self._handler.iter(Tape),
            self._handler.iter(ArchiveRoute),
        ):
            if obj.tape_pool_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {obj} ",
                    2,
                    obj,
                )
        for obj in self._handler.iter(TapePool):
            if name in obj.supply_tape_pool_names:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {obj} ",
                    2,
                    obj,
                )
        self._handler.delete(self.resource_type, name)

    @override
    def list(
        self,
        /,
        *,
        virtual_organisation_name: Optional[str] = None,
        encrypted: Optional[bool] = None,
    ) -> Iterator[TapePool]:
        yield from (
            tape_pool
            for tape_pool in self._handler.iter(self.resource_type)
            if (virtual_organisation_name is None or tape_pool.virtual_organisation_name == virtual_organisation_name)
            and (encrypted is None or (tape_pool.encryption_key_name is not None) == encrypted)
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
        with self._handler.update(self.resource_type, name) as tape_pool:
            if virtual_organisation_name is not None:
                try:
                    tape_pool.virtual_organisation_name = self._handler.get(
                        VirtualOrganisation, virtual_organisation_name
                    ).name
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e
            if min_writable_tape_count is not None:
                tape_pool.min_writable_tape_count = min_writable_tape_count
            if supply_tape_pool_names is not None:
                try:
                    tape_pool.supply_tape_pool_names = [
                        self._handler.get(TapePool, tp_name).name for tp_name in supply_tape_pool_names
                    ]
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e

            tape_pool.metadata = self._handler.refresh_metadata(tape_pool.metadata, comment=comment)

    @override
    def remove_encryption(self, name: str, /) -> None:
        with self._handler.update(self.resource_type, name) as tape_pool:
            tape_pool.encryption_key_name = None
            tape_pool.metadata = self._handler.refresh_metadata(tape_pool.metadata)

    @override
    def set_encryption_key_name(self, name: str, /, *, encryption_key_name: str) -> None:
        with self._handler.update(self.resource_type, name) as tape_pool:
            tape_pool.encryption_key_name = encryption_key_name
            tape_pool.metadata = self._handler.refresh_metadata(tape_pool.metadata)
