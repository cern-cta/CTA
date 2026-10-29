# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import MediaTypeDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler: CtaFileHandler) -> MediaTypeDaoFile:
    return MediaTypeDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao: MediaTypeDaoFile):
        mt = dao.get("LTO8")
        assert mt.cartridge_type == "LTO-8"
        assert mt.capacity == 12_000_000_000_000

    def test_get_missing_raises(self, dao: MediaTypeDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao: MediaTypeDaoFile):
        assert "LTO8" in {mt.name for mt in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao: MediaTypeDaoFile):
        dao.create("LTO9", cartridge_type="LTO-9", capacity=18_000_000_000_000, primary_density_code=3)
        mt = dao.get("LTO9")
        assert mt.capacity == 18_000_000_000_000
        assert mt.primary_density_code == 3

    def test_create_duplicate_raises(self, dao: MediaTypeDaoFile):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("LTO8", cartridge_type="x", capacity=1)


class TestUpdate:
    def test_update_fields(self, dao: MediaTypeDaoFile):
        dao.update("LTO8", cartridge_type="LTO-8-bis", secondary_density_code=9)
        mt = dao.get("LTO8")
        assert mt.cartridge_type == "LTO-8-bis"
        assert mt.secondary_density_code == 9

    def test_update_missing_raises(self, dao: MediaTypeDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", cartridge_type="x")


class TestDelete:
    def test_delete_unreferenced(self, dao: MediaTypeDaoFile):
        dao.create("MT_free", cartridge_type="x", capacity=1)
        dao.delete("MT_free")
        with pytest.raises(ResourceNotFoundError):
            dao.get("MT_free")

    def test_delete_referenced_raises_backend_error(self, dao: MediaTypeDaoFile):
        # LTO8 is referenced by the seeded tapes.
        with pytest.raises(BackendError):
            dao.delete("LTO8")
