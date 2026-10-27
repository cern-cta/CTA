# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import ArchiveRouteDaoFile
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import ArchiveRouteType

DEFAULT = ArchiveRouteType.DEFAULT
REPACK = ArchiveRouteType.REPACK


@pytest.fixture
def dao(handler) -> ArchiveRouteDaoFile:
    return ArchiveRouteDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao):
        route = dao.get("sc_ci", 1, DEFAULT)
        assert route.tape_pool_name == "tp_ci"

    def test_get_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get("sc_ci", 2, DEFAULT)


class TestList:
    def test_list_contains_seed(self, dao):
        assert ("sc_ci", 1, DEFAULT) in {(r.storage_class_name, r.copy_number, r.type) for r in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao):
        dao.create("sc_ci", 2, REPACK, tape_pool_name="tp_ci")
        assert dao.get("sc_ci", 2, REPACK).tape_pool_name == "tp_ci"

    def test_create_stored_with_enum_name_in_filename(self, dao, data_root):
        dao.create("sc_ci", 2, REPACK, tape_pool_name="tp_ci")
        assert (data_root / "archiveroute" / "sc_ci__2__REPACK.json").is_file()

    def test_create_missing_storage_class_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create("ghost", 1, DEFAULT, tape_pool_name="tp_ci")

    def test_create_missing_tape_pool_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create("sc_ci", 3, DEFAULT, tape_pool_name="ghost")

    def test_create_duplicate_raises(self, dao):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("sc_ci", 1, DEFAULT, tape_pool_name="tp_ci")


class TestUpdate:
    def test_update_comment(self, dao):
        dao.update("sc_ci", 1, DEFAULT, comment="c2")
        assert dao.get("sc_ci", 1, DEFAULT).metadata.comment == "c2"

    def test_update_tape_pool(self, dao):
        dao.update("sc_ci", 1, DEFAULT, tape_pool_name="tp_encrypted")
        assert dao.get("sc_ci", 1, DEFAULT).tape_pool_name == "tp_encrypted"

    def test_update_missing_tape_pool_raises(self, dao):
        with pytest.raises(BackendError):
            dao.update("sc_ci", 1, DEFAULT, tape_pool_name="ghost")

    def test_update_missing_resource_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.update("sc_ci", 99, DEFAULT, comment="x")


class TestDelete:
    def test_delete(self, dao):
        dao.delete("sc_ci", 1, DEFAULT)
        with pytest.raises(ResourceNotFoundError):
            dao.get("sc_ci", 1, DEFAULT)

    def test_delete_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("sc_ci", 2, DEFAULT)
