# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import VirtualOrganisationDaoFile
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler) -> VirtualOrganisationDaoFile:
    return VirtualOrganisationDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao):
        vo = dao.get("vo_ci")
        assert vo.disk_instance_name == "instance_ci"
        assert vo.max_read_drives == 2
        assert vo.max_write_drives == 3

    def test_get_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao):
        assert "vo_ci" in {vo.name for vo in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao):
        dao.create("vo_new", disk_instance_name="instance_ci", max_read_drives=7)
        assert dao.get("vo_new").max_read_drives == 7

    def test_create_missing_disk_instance_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create("vo_bad", disk_instance_name="ghost")

    def test_create_duplicate_raises(self, dao):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("vo_ci", disk_instance_name="instance_ci")


class TestUpdate:
    def test_update_fields(self, dao):
        dao.update("vo_ci", is_repack_vo=True, max_file_size=999, comment="c2")
        vo = dao.get("vo_ci")
        assert vo.is_repack_vo is True
        assert vo.max_file_size == 999
        assert vo.metadata.comment == "c2"

    def test_update_missing_disk_instance_raises(self, dao):
        with pytest.raises(BackendError):
            dao.update("vo_ci", disk_instance_name="ghost")

    def test_update_missing_resource_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", comment="x")


class TestDelete:
    def test_delete_unreferenced(self, dao):
        dao.create("vo_new", disk_instance_name="instance_ci")
        dao.delete("vo_new")
        with pytest.raises(ResourceNotFoundError):
            dao.get("vo_new")

    def test_delete_referenced_raises_backend_error(self, dao):
        # vo_ci is referenced by storage classes and tape pools.
        with pytest.raises(BackendError):
            dao.delete("vo_ci")
