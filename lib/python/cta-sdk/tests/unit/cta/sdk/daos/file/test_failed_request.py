# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import FailedRequestDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import MountType


@pytest.fixture
def dao(handler: CtaFileHandler) -> FailedRequestDaoFile:
    return FailedRequestDaoFile(handler)


class TestList:
    def test_list_all(self, dao: FailedRequestDaoFile):
        assert {fr.id for fr in dao.list()} == {"fr_archive", "fr_retrieve"}

    def test_filter_by_type(self, dao: FailedRequestDaoFile):
        assert {fr.id for fr in dao.list(type=MountType.ARCHIVE)} == {"fr_archive"}

    def test_filter_by_tape_pool(self, dao: FailedRequestDaoFile):
        assert {fr.id for fr in dao.list(tape_pool_name="tp_ci")} == {"fr_archive", "fr_retrieve"}

    def test_filter_by_vid(self, dao: FailedRequestDaoFile):
        assert {fr.id for fr in dao.list(vid="V00002")} == {"fr_retrieve"}

    def test_logs_withheld_by_default(self, dao: FailedRequestDaoFile):
        assert {fr.logs for fr in dao.list()} == {""}

    def test_logs_included_when_requested(self, dao: FailedRequestDaoFile):
        assert {fr.logs for fr in dao.list(logs=True)} == {"archive boom", "retrieve boom"}


class TestSummary:
    def test_summary_groups_by_mount_type(self, dao: FailedRequestDaoFile):
        summaries = {s.mount_type: s for s in dao.summary()}
        assert set(summaries) == {MountType.ARCHIVE, MountType.RETRIEVE}
        assert summaries[MountType.ARCHIVE].total_file_count == 1
        assert summaries[MountType.ARCHIVE].total_byte_count == 1024


class TestDelete:
    def test_delete(self, dao: FailedRequestDaoFile):
        dao.delete("fr_archive")
        assert {fr.id for fr in dao.list()} == {"fr_retrieve"}

    def test_delete_missing_raises(self, dao: FailedRequestDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("ghost")
