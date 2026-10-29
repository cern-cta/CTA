# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta

import pytest
from cta.sdk.daos.file import RecycleBinTapeFileDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import ResourceNotFoundError

from .conftest import SEED_UPDATE_TIME


@pytest.fixture
def dao(handler: CtaFileHandler) -> RecycleBinTapeFileDaoFile:
    return RecycleBinTapeFileDaoFile(handler)


class TestGetAllCopies:
    def test_by_archive_id(self, dao: RecycleBinTapeFileDaoFile):
        copies = dao.get_all_copies(200)
        assert len(copies) == 1
        assert copies[0].deletion_reason == "user delete"

    def test_by_disk_file(self, dao: RecycleBinTapeFileDaoFile):
        assert len(dao.get_all_copies("df200", "instance_ci")) == 1

    def test_unknown_returns_empty(self, dao: RecycleBinTapeFileDaoFile):
        assert dao.get_all_copies(999) == ()


class TestGet:
    def test_by_archive_id_and_vid(self, dao: RecycleBinTapeFileDaoFile):
        recycled = dao.get(200, vid="V00001")
        assert recycled.virtual_organisation_name == "vo_ci"

    def test_missing_raises(self, dao: RecycleBinTapeFileDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get(200, vid="V99999")


class TestList:
    def test_list_by_vid(self, dao: RecycleBinTapeFileDaoFile):
        assert len(list(dao.list(vid="V00001"))) == 1

    def test_list_by_vo(self, dao: RecycleBinTapeFileDaoFile):
        assert len(list(dao.list(virtual_organisation_name="vo_ci"))) == 1

    def test_list_by_vo_no_match(self, dao: RecycleBinTapeFileDaoFile):
        assert list(dao.list(virtual_organisation_name="other")) == []

    def test_list_min_deletion_time_datetime(self, dao: RecycleBinTapeFileDaoFile):
        assert len(list(dao.list(vid="V00001", min_deletion_time=SEED_UPDATE_TIME - timedelta(days=1)))) == 1
        assert list(dao.list(vid="V00001", min_deletion_time=SEED_UPDATE_TIME + timedelta(days=1))) == []

    def test_list_max_deletion_time_epoch(self, dao: RecycleBinTapeFileDaoFile):
        before = int((SEED_UPDATE_TIME - timedelta(days=1)).timestamp())
        assert list(dao.list(vid="V00001", max_deletion_time=before)) == []

    def test_list_requires_a_selector(self, dao: RecycleBinTapeFileDaoFile):
        with pytest.raises(TypeError):
            list(dao.list(min_deletion_time=0))
