# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta

import pytest
from cta.sdk.daos.file import MountPolicyDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler: CtaFileHandler) -> MountPolicyDaoFile:
    return MountPolicyDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao: MountPolicyDaoFile):
        mp = dao.get("mp_ci")
        assert mp.archive_priority == 250
        assert mp.min_archive_age == timedelta(hours=4)

    def test_get_missing_raises(self, dao: MountPolicyDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao: MountPolicyDaoFile):
        assert "mp_ci" in {mp.name for mp in dao.list()}


class TestCreate:
    def test_create_with_defaults(self, dao: MountPolicyDaoFile):
        dao.create("mp_new")
        mp = dao.get("mp_new")
        assert mp.archive_priority == 250
        assert mp.min_archive_age == timedelta(seconds=14_400)

    def test_create_int_ages_become_timedelta(self, dao: MountPolicyDaoFile):
        dao.create("mp_ints", min_archive_age=60, min_retrieve_age=120)
        mp = dao.get("mp_ints")
        assert mp.min_archive_age == timedelta(seconds=60)
        assert mp.min_retrieve_age == timedelta(seconds=120)

    def test_create_duplicate_raises(self, dao: MountPolicyDaoFile):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("mp_ci")


class TestUpdate:
    def test_update_fields(self, dao: MountPolicyDaoFile):
        dao.update("mp_ci", archive_priority=1, min_retrieve_age=timedelta(minutes=5))
        mp = dao.get("mp_ci")
        assert mp.archive_priority == 1
        assert mp.min_retrieve_age == timedelta(minutes=5)

    def test_update_missing_raises(self, dao: MountPolicyDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", archive_priority=1)


class TestDelete:
    def test_delete_unreferenced(self, dao: MountPolicyDaoFile):
        dao.create("mp_free")
        dao.delete("mp_free")
        with pytest.raises(ResourceNotFoundError):
            dao.get("mp_free")

    def test_delete_referenced_raises_backend_error(self, dao: MountPolicyDaoFile):
        # mp_ci is referenced by user/group/activity mount rules.
        with pytest.raises(BackendError):
            dao.delete("mp_ci")
