# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import UserRequestDaoFile
from cta.sdk.models import MountType


@pytest.fixture
def dao(handler) -> UserRequestDaoFile:
    return UserRequestDaoFile(handler)


class TestList:
    def test_list_all(self, dao):
        assert len(list(dao.list())) == 2

    def test_filter_archive(self, dao):
        reqs = list(dao.list(request_type=MountType.ARCHIVE))
        assert len(reqs) == 1
        assert reqs[0].mount_type is MountType.ARCHIVE
        assert reqs[0].vid is None

    def test_filter_retrieve(self, dao):
        reqs = list(dao.list(request_type=MountType.RETRIEVE))
        assert len(reqs) == 1
        assert reqs[0].vid == "V00001"


class TestSummary:
    def test_summary_static_helper(self, dao):
        reqs = list(dao.list(request_type=MountType.RETRIEVE))
        summaries = UserRequestDaoFile.summary(reqs, group_by="tape_pool_name")
        assert len(summaries) == 1
        summary = summaries[0]
        assert summary.group_value == "tp_ci"
        assert summary.queued_file_count == 4
        assert summary.tapes_count == 1

    def test_summary_rejects_mixed_mount_types(self, dao):
        with pytest.raises(ValueError):
            UserRequestDaoFile.summary(list(dao.list()), group_by="tape_pool_name")

    def test_summary_empty(self, dao):
        assert UserRequestDaoFile.summary([], group_by="tape_pool_name") == []
