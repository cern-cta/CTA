# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
from typing import Any, Callable, Iterator

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import TapeFileDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import TapeFile


def file_identity(*args: Any) -> tuple[Callable[[TapeFile], bool], dict[str, Any]]:
    """Resolve the "by archive id" / "by disk file" overloads into a predicate.

    Returns:
        The predicate and the identity as a dict, for error reporting.
    """
    if len(args) == 1 and isinstance(args[0], int):
        (archive_id,) = args
        return (lambda f: f.archive_id == archive_id), {"archive_id": archive_id}

    if len(args) == 2:
        disk_file_id, disk_instance_name = args
        return (lambda f: f.disk_file.id == disk_file_id and f.disk_file.disk_instance_name == disk_instance_name), {
            "disk_file_id": disk_file_id,
            "disk_instance_name": disk_instance_name,
        }

    raise TypeError(f"Expected (archive_id) or (disk_file_id, disk_instance_name), got {args!r}")


def read_fids_file(fids_file: str) -> set[str]:
    """Read the disk file ids listed (line separated) in `fids_file`."""
    return set(Path(fids_file).read_text().split("\n"))


class TapeFileDaoFile(TapeFileDao):
    """File implementation of the tape file Dao.

    Note:
        Tape files are keyed on disk by (archive_id, vid, fseq), but the Dao looks them up by
        (archive_id, vid) or by disk file, so every operation scans the directory rather than
        addressing a file directly. Prefer `list` over repeated `get`, as with the CLI backend.
    """

    resource_type = TapeFile

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def get_all_copies(self, *args: Any, **kwargs: Any) -> tuple[TapeFile, ...]:
        matches, _ = file_identity(*args, **kwargs)
        return tuple(tape_file for tape_file in self._handler.iter(self.resource_type) if matches(tape_file))

    def get(self, *args: Any, vid: str) -> TapeFile:
        matches, identity = file_identity(*args)
        try:
            return next(
                tape_file
                for tape_file in self._handler.iter(self.resource_type)
                if matches(tape_file) and tape_file.vid == vid
            )
        except StopIteration as e:
            raise ResourceNotFoundError(
                self._handler.class_to_dir_name(self.resource_type), {**identity, "vid": vid}
            ) from e

    def list(self, **kwargs: Any) -> Iterator[TapeFile]:
        if "vid" in kwargs:
            vid = kwargs["vid"]
            yield from (tape_file for tape_file in self._handler.iter(self.resource_type) if tape_file.vid == vid)
        elif "fids_file" in kwargs and "disk_instance_name" in kwargs:
            disk_file_ids = read_fids_file(kwargs["fids_file"])
            disk_instance_name = kwargs["disk_instance_name"]
            yield from (
                tape_file
                for tape_file in self._handler.iter(self.resource_type)
                if tape_file.disk_file.disk_instance_name == disk_instance_name
                and tape_file.disk_file.id in disk_file_ids
            )
        else:
            raise TypeError(f"Expected 'vid' or ('fids_file', 'disk_instance_name'), got {sorted(kwargs)}")

    def delete(self, *args: Any, vid: str, reason: str) -> None:
        copies = self.get_all_copies(*args)
        if len(copies) == 0:
            raise ResourceNotFoundError("TapeFile", file_identity(*args)[1])
        elif len(copies) == 1:
            raise BackendError("This file has only one copy", 1, args)

        tape_file = self.get(*args, vid=vid)
        self._handler.delete(self.resource_type, tape_file.archive_id, tape_file.vid, tape_file.fseq)
