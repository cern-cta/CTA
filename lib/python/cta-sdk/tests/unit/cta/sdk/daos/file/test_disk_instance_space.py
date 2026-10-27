# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta

import pytest
from cta.sdk.daos.file import DiskInstanceSpaceDaoFile
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler) -> DiskInstanceSpaceDaoFile:
    return DiskInstanceSpaceDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao):
        space = dao.get("space_ci", "instance_ci")
        assert space.free_space_url == "eos:ci"
        assert space.refresh_interval == timedelta(seconds=30)

    def test_get_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost", "instance_ci")


class TestList:
    def test_list_contains_seed(self, dao):
        assert ("space_ci", "instance_ci") in {(s.name, s.disk_instance_name) for s in dao.list()}


class TestCreate:
    def test_create_int_refresh_interval_becomes_timedelta(self, dao):
        dao.create("space_new", "instance_ci", free_space_url="eos:new", refresh_interval=45)
        assert dao.get("space_new", "instance_ci").refresh_interval == timedelta(seconds=45)

    def test_create_timedelta_refresh_interval(self, dao):
        dao.create("space_td", "instance_ci", free_space_url="u", refresh_interval=timedelta(minutes=2))
        assert dao.get("space_td", "instance_ci").refresh_interval == timedelta(minutes=2)

    def test_create_missing_disk_instance_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create("space_bad", "ghost", free_space_url="u")

    def test_create_duplicate_raises(self, dao):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("space_ci", "instance_ci", free_space_url="u")


class TestUpdate:
    def test_update_fields(self, dao):
        dao.update("space_ci", "instance_ci", free_space_url="eos:changed", refresh_interval=99)
        space = dao.get("space_ci", "instance_ci")
        assert space.free_space_url == "eos:changed"
        assert space.refresh_interval == timedelta(seconds=99)

    def test_update_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", "instance_ci", free_space_url="u")


class TestDelete:
    def test_delete_unreferenced(self, dao):
        dao.create("space_new", "instance_ci", free_space_url="u")
        dao.delete("space_new", "instance_ci")
        with pytest.raises(ResourceNotFoundError):
            dao.get("space_new", "instance_ci")

    def test_delete_referenced_raises_backend_error(self, dao):
        # space_ci is referenced by the seeded disk system.
        with pytest.raises(BackendError):
            dao.delete("space_ci", "instance_ci")
