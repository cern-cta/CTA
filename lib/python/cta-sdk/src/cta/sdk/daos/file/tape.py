# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from itertools import chain
from typing import Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import TapeDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import (
    LogicalLibrary,
    MediaType,
    RecycleBinTapeFile,
    Tape,
    TapeFile,
    TapeMetadata,
    TapePool,
    TapeState,
    TapeStatistics,
    VerificationStatus,
)
from typing_extensions import override


class TapeDaoFile(TapeDao):
    resource_type = Tape

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

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
        try:
            logical_library = self._handler.get(LogicalLibrary, logical_library_name)
            tape_pool = self._handler.get(TapePool, tape_pool_name)
            media_type = self._handler.get(MediaType, media_type_name)

        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e

        tape = Tape(
            vid,
            media_type_name=media_type_name,
            vendor=vendor,
            purchase_order=purchase_order or "",
            logical_library_name=logical_library_name,
            physical_library_name=logical_library.physical_library_name,
            tape_pool_name=tape_pool_name,
            virtual_organisation_name=tape_pool.virtual_organisation_name,
            encryption_key_name=tape_pool.encryption_key_name,
            capacity=media_type.capacity,
            format="",
            state=TapeState.ACTIVE,
            state_reason="",
            verification_status=None,
            statistics=TapeStatistics(
                physical_usage=0,
                logical_usage=0,
                is_dirty=False,
                last_fseq=0,
                is_full=full,
                file_count=0,
            ),
            metadata=TapeMetadata(
                **self._handler.gen_creation_metadata(),
                **self._handler.gen_update_metadata(),
                comment=comment,
                label_time=None,
                label_drive_name=None,
                last_write_time=None,
                last_write_drive_name=None,
                last_read_time=None,
                last_read_drive_name=None,
                update_state_time=datetime.now(tz=timezone.utc),
                update_state_info="",
                write_mount_count=0,
                read_mount_count=0,
            ),
        )
        self._handler.create(tape)

    @override
    def get(self, vid: str, /) -> Tape:
        return self._handler.get(self.resource_type, vid)

    @override
    def delete(self, vid: str, /) -> None:
        for obj in chain(
            self._handler.iter(TapeFile),
            self._handler.iter(RecycleBinTapeFile),
        ):
            if obj.vid == vid:
                raise BackendError(
                    f"You can not delete {self.resource_type}({vid}), it is referenced by {obj} ",
                    2,
                    obj,
                )

        self._handler.delete(self.resource_type, vid)

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
        if fxid_file is not None or missing_file_copies:
            raise NotImplementedError(
                "'fxid_file' and 'missing_file_copies' filters need the archive file catalogue, "
                "which the file backend does not model"
            )

        yield from (
            tape
            for tape in self._handler.iter(self.resource_type)
            if (media_type_name is None or tape.media_type_name == media_type_name)
            and (vendor is None or tape.vendor == vendor)
            and (logical_library_name is None or tape.logical_library_name == logical_library_name)
            and (tape_pool_name is None or tape.tape_pool_name == tape_pool_name)
            and (virtual_organisation_name is None or tape.virtual_organisation_name == virtual_organisation_name)
            and (full is None or tape.statistics.is_full == full)
            and (state is None or tape.state == state)
            and (purchase_order is None or tape.purchase_order == purchase_order)
            and (physical_library_name is None or tape.physical_library_name == physical_library_name)
        )

    def _update_encryption_key(self, tape: Tape, encryption_key_name: str) -> None:
        """Reject key changes on nonempty tapes before updating the key."""
        if (
            tape.encryption_key_name
            and tape.encryption_key_name != encryption_key_name
            and tape.statistics.file_count != 0
        ):
            raise BackendError(
                f"Could not change an encryption key on a none empty tape: {self.resource_type}({tape.vid})", 2, {}
            )
        tape.encryption_key_name = encryption_key_name

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
        with self._handler.update(self.resource_type, vid) as tape:
            if media_type_name is not None:
                try:
                    tape.capacity = self._handler.get(MediaType, media_type_name).capacity
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e
            if vendor is not None:
                tape.vendor = vendor
            if logical_library_name is not None:
                try:
                    logical_library = self._handler.get(LogicalLibrary, logical_library_name)
                    tape.logical_library_name = logical_library.name
                    tape.physical_library_name = logical_library.physical_library_name

                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e

            if tape_pool_name is not None:
                try:
                    tape_pool = self._handler.get(TapePool, tape_pool_name)
                    tape.tape_pool_name = tape_pool.name
                    tape.virtual_organisation_name = tape_pool.virtual_organisation_name
                    # We don't update the encryption_key_name on purpose here
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e

            if verification_status is not None:
                tape.verification_status = verification_status
            if full is not None:
                tape.statistics.is_full = full
            if purchase_order is not None:
                tape.purchase_order = purchase_order
            if dirty is not None:
                tape.statistics.is_dirty = dirty
            if encryption_key_name is not None:
                self._update_encryption_key(tape, encryption_key_name)

            tape.metadata = self._handler.refresh_metadata(tape.metadata, comment=comment)

    @override
    def reclaim(self, vid: str, /) -> None:
        with self._handler.update(self.resource_type, vid) as tape:
            tape.statistics = TapeStatistics(
                physical_usage=0,
                logical_usage=0,
                is_dirty=False,
                last_fseq=0,
                is_full=False,
                file_count=0,
            )
            tape.metadata = self._handler.refresh_metadata(tape.metadata)

    @override
    def change_state(self, vid: str, /, *, state: TapeState, reason: str) -> None:
        with self._handler.update(self.resource_type, vid) as tape:
            tape.state = state
            tape.state_reason = reason
            tape.metadata = self._handler.refresh_metadata(tape.metadata)
            tape.metadata.update_state_time = datetime.now(tz=timezone.utc)
            tape.metadata.update_state_info = reason
