# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.mock import StorageClassDaoMock
from cta.sdk.daos.mock._handler import CtaMockHandler
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler: CtaMockHandler) -> StorageClassDaoMock:
    return StorageClassDaoMock(handler)


class TestGet:
    def test_get_seeded(self, dao: StorageClassDaoMock):
        sc = dao.get("sc_ci")
        assert sc.virtual_organisation_name == "vo_ci"
        assert sc.replica_count == 2

    def test_get_missing_raises(self, dao: StorageClassDaoMock):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao: StorageClassDaoMock):
        assert "sc_ci" in {sc.name for sc in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao: StorageClassDaoMock):
        dao.create("sc_new", virtual_organisation_name="vo_ci", replica_count=3)
        assert dao.get("sc_new").replica_count == 3

    def test_create_missing_vo_raises(self, dao: StorageClassDaoMock):
        with pytest.raises(BackendError):
            dao.create("sc_bad", virtual_organisation_name="ghost")

    def test_create_duplicate_raises(self, dao: StorageClassDaoMock):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("sc_ci", virtual_organisation_name="vo_ci")


class TestUpdate:
    def test_update_replica_count(self, dao: StorageClassDaoMock):
        dao.update("sc_ci", replica_count=5)
        assert dao.get("sc_ci").replica_count == 5

    def test_update_missing_vo_raises(self, dao: StorageClassDaoMock):
        with pytest.raises(BackendError):
            dao.update("sc_ci", virtual_organisation_name="ghost")

    def test_update_missing_resource_raises(self, dao: StorageClassDaoMock):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", replica_count=1)


class TestDelete:
    def test_delete_unreferenced(self, dao: StorageClassDaoMock):
        dao.create("sc_new", virtual_organisation_name="vo_ci")
        dao.delete("sc_new")
        with pytest.raises(ResourceNotFoundError):
            dao.get("sc_new")

    def test_delete_referenced_raises_backend_error(self, dao: StorageClassDaoMock):
        # sc_ci is referenced by tape files, recycle-bin files and an archive route.
        with pytest.raises(BackendError):
            dao.delete("sc_ci")
