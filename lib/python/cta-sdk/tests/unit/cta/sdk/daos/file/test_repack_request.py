# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta

import pytest
from cta.sdk.daos.file import RepackRequestDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import RepackState


@pytest.fixture
def dao(handler: CtaFileHandler) -> RepackRequestDaoFile:
    return RepackRequestDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao: RepackRequestDaoFile):
        repack = dao.get("V00002")
        assert repack.tape_pool_name == "tp_ci"
        assert repack.state is RepackState.PENDING

    def test_get_missing_raises(self, dao: RepackRequestDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao: RepackRequestDaoFile):
        assert "V00002" in {r.vid for r in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao: RepackRequestDaoFile):
        # V00001 is a seeded tape with no repack request yet.
        dao.create("V00001", mount_policy_name="mp_ci", buffer_url="root://buf")
        repack = dao.get("V00001")
        assert repack.tape_pool_name == "tp_ci"
        assert repack.disk_buffer_url == "root://buf"
        assert repack.duration == timedelta(0)
        assert repack.state is RepackState.PENDING

    def test_create_missing_tape_raises(self, dao: RepackRequestDaoFile):
        # The linked tape must already exist; the handler raises when it does not.
        with pytest.raises(ResourceNotFoundError):
            dao.create("V99999", mount_policy_name="mp_ci")

    def test_create_duplicate_raises(self, dao: RepackRequestDaoFile):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("V00002", mount_policy_name="mp_ci")


class TestDelete:
    def test_delete(self, dao: RepackRequestDaoFile):
        dao.delete("V00002")
        with pytest.raises(ResourceNotFoundError):
            dao.get("V00002")

    def test_delete_missing_raises(self, dao: RepackRequestDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("ghost")
