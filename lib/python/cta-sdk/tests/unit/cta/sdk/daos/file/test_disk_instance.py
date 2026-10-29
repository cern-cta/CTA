# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.file import DiskInstanceDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import TapeFile

from .conftest import SEED_CREATION_TIME


@pytest.fixture
def dao(handler: CtaFileHandler) -> DiskInstanceDaoFile:
    return DiskInstanceDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao: DiskInstanceDaoFile):
        assert dao.get("instance_ci").name == "instance_ci"

    def test_get_missing_raises(self, dao: DiskInstanceDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao: DiskInstanceDaoFile):
        assert "instance_ci" in {di.name for di in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao: DiskInstanceDaoFile):
        dao.create("instance_new", comment="new")
        assert dao.get("instance_new").metadata.comment == "new"

    def test_create_duplicate_raises(self, dao: DiskInstanceDaoFile):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("instance_ci")


class TestUpdate:
    def test_update_comment(self, dao: DiskInstanceDaoFile):
        dao.update("instance_ci", comment="changed")
        assert dao.get("instance_ci").metadata.comment == "changed"

    def test_update_preserves_creation(self, dao: DiskInstanceDaoFile):
        dao.update("instance_ci", comment="changed")
        assert dao.get("instance_ci").metadata.creation_time == SEED_CREATION_TIME

    def test_update_missing_raises(self, dao: DiskInstanceDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", comment="x")


class TestDelete:
    def test_delete_unreferenced(self, dao: DiskInstanceDaoFile):
        dao.create("instance_new")
        dao.delete("instance_new")
        with pytest.raises(ResourceNotFoundError):
            dao.get("instance_new")

    def test_delete_referenced_raises_backend_error(self, dao: DiskInstanceDaoFile):
        # instance_ci is referenced by mount rules, spaces and the virtual organisation.
        with pytest.raises(BackendError):
            dao.delete("instance_ci")

    def test_delete_missing_raises(self, dao: DiskInstanceDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("ghost")


def test_delete_reports_tape_file_reference() -> None:
    handler = MagicMock(spec=CtaFileHandler)
    tape_file = MagicMock(spec=TapeFile)
    tape_file.disk_file = MagicMock(disk_instance_name="instance_ci")
    # No earlier resource references exist, so only the tape-file loop runs.
    handler.iter.side_effect = [iter(()) for _ in range(5)] + [iter((tape_file,))]

    with pytest.raises(BackendError) as error:
        DiskInstanceDaoFile(handler).delete("instance_ci")

    assert error.value.request_info is tape_file
    handler.delete.assert_not_called()
