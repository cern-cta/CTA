# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Union

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.file.tape_file import file_identity, read_fids_file
from cta.sdk.daos.protocols import RecycleBinTapeFileDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import RecycleBinTapeFile
from typing_extensions import override


def _as_datetime(value: Union[datetime, int]) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromtimestamp(value, tz=timezone.utc)


class RecycleBinTapeFileDaoFile(RecycleBinTapeFileDao):
    """File implementation of the recycle bin tape file Dao.

    Note:
        `RecycleBinTapeFileDao` is read-only, entries have to be seeded through `CtaFileHandler.create`.
    """

    resource_type = RecycleBinTapeFile

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    @override
    def get_all_copies(self, *args: Any, **kwargs: Any) -> tuple[RecycleBinTapeFile, ...]:
        matches, _ = file_identity(*args, **kwargs)
        return tuple(recycled for recycled in self._handler.iter(self.resource_type) if matches(recycled))

    @override
    def get(self, *args: Any, vid: str) -> RecycleBinTapeFile:
        matches, identity = file_identity(*args)
        try:
            return next(
                recycled
                for recycled in self._handler.iter(self.resource_type)
                if matches(recycled) and recycled.vid == vid
            )
        except StopIteration as e:
            raise ResourceNotFoundError(
                self._handler.class_to_dir_name(self.resource_type), {**identity, "vid": vid}
            ) from e

    @override
    def list(self, **kwargs: Any) -> Iterator[RecycleBinTapeFile]:
        vid = kwargs.get("vid")
        fids_file = kwargs.get("fids_file")
        disk_instance_name = kwargs.get("disk_instance_name")
        virtual_organisation_name = kwargs.get("virtual_organisation_name")
        min_deletion_time = kwargs.get("min_deletion_time")
        max_deletion_time = kwargs.get("max_deletion_time")

        if vid is None and virtual_organisation_name is None and fids_file is None:
            raise TypeError(
                f"Expected 'vid', 'virtual_organisation_name' or ('fids_file', 'disk_instance_name'), "
                f"got {sorted(kwargs)}"
            )
        if (fids_file is None) != (disk_instance_name is None):
            raise TypeError("'fids_file' and 'disk_instance_name' go together")

        disk_file_ids = None if fids_file is None else read_fids_file(fids_file)
        min_time = None if min_deletion_time is None else _as_datetime(min_deletion_time)
        max_time = None if max_deletion_time is None else _as_datetime(max_deletion_time)

        yield from (
            recycled
            for recycled in self._handler.iter(self.resource_type)
            if (vid is None or recycled.vid == vid)
            and (virtual_organisation_name is None or recycled.virtual_organisation_name == virtual_organisation_name)
            and (
                disk_file_ids is None
                or (
                    recycled.disk_file.disk_instance_name == disk_instance_name
                    and recycled.disk_file.id in disk_file_ids
                )
            )
            and (min_time is None or recycled.deletion_time >= min_time)
            and (max_time is None or recycled.deletion_time <= max_time)
        )
