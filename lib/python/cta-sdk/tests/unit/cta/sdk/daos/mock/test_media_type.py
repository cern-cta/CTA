# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.mock import MediaTypeDaoMock
from cta.sdk.daos.mock._handler import CtaMockHandler
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler: CtaMockHandler) -> MediaTypeDaoMock:
    return MediaTypeDaoMock(handler)


class TestGet:
    def test_get_seeded(self, dao: MediaTypeDaoMock):
        mt = dao.get("LTO8")
        assert mt.cartridge_type == "LTO-8"
        assert mt.capacity == 12_000_000_000_000

    def test_get_missing_raises(self, dao: MediaTypeDaoMock):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao: MediaTypeDaoMock):
        assert "LTO8" in {mt.name for mt in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao: MediaTypeDaoMock):
        dao.create("LTO9", cartridge_type="LTO-9", capacity=18_000_000_000_000, primary_density_code=3)
        mt = dao.get("LTO9")
        assert mt.capacity == 18_000_000_000_000
        assert mt.primary_density_code == 3

    def test_create_duplicate_raises(self, dao: MediaTypeDaoMock):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("LTO8", cartridge_type="x", capacity=1)


class TestUpdate:
    def test_update_fields(self, dao: MediaTypeDaoMock):
        dao.update("LTO8", cartridge_type="LTO-8-bis", secondary_density_code=9)
        mt = dao.get("LTO8")
        assert mt.cartridge_type == "LTO-8-bis"
        assert mt.secondary_density_code == 9

    def test_update_missing_raises(self, dao: MediaTypeDaoMock):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", cartridge_type="x")


class TestDelete:
    def test_delete_unreferenced(self, dao: MediaTypeDaoMock):
        dao.create("MT_free", cartridge_type="x", capacity=1)
        dao.delete("MT_free")
        with pytest.raises(ResourceNotFoundError):
            dao.get("MT_free")

    def test_delete_referenced_raises_backend_error(self, dao: MediaTypeDaoMock):
        # LTO8 is referenced by the seeded tapes.
        with pytest.raises(BackendError):
            dao.delete("LTO8")
