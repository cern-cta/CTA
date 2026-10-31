# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import PhysicalLibraryDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import PhysicalLibraryState


@pytest.fixture
def dao(handler: CtaFileHandler) -> PhysicalLibraryDaoFile:
    return PhysicalLibraryDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao: PhysicalLibraryDaoFile):
        pl = dao.get("physlib_ci")
        assert pl.manufacturer == "IBM"
        assert pl.physical_cartridge_slots == 1000
        assert pl.state is PhysicalLibraryState.ENABLED

    def test_get_missing_raises(self, dao: PhysicalLibraryDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao: PhysicalLibraryDaoFile):
        assert "physlib_ci" in {pl.name for pl in dao.list()}

    def test_list_filter_enabled(self, dao: PhysicalLibraryDaoFile):
        assert "physlib_ci" in {pl.name for pl in dao.list(state=PhysicalLibraryState.ENABLED)}

    def test_list_filter_disabled_empty(self, dao: PhysicalLibraryDaoFile):
        assert list(dao.list(state=PhysicalLibraryState.DISABLED)) == []


class TestCreate:
    def test_create_with_optional_defaults(self, dao: PhysicalLibraryDaoFile):
        dao.create(
            "pl_new", manufacturer="Spectra", model="TFinity", physical_cartridge_slots=10, physical_drive_slots=2
        )
        pl = dao.get("pl_new")
        assert pl.type == ""
        assert pl.gui_url == ""
        # available_cartridge_slots defaults to physical_cartridge_slots when omitted.
        assert pl.available_cartridge_slots == 10
        assert pl.state is PhysicalLibraryState.ENABLED

    def test_create_duplicate_raises(self, dao: PhysicalLibraryDaoFile):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("physlib_ci", manufacturer="x", model="y", physical_cartridge_slots=1, physical_drive_slots=1)


class TestUpdate:
    def test_update_fields(self, dao: PhysicalLibraryDaoFile):
        dao.update("physlib_ci", location="B742", available_cartridge_slots=5)
        pl = dao.get("physlib_ci")
        assert pl.location == "B742"
        assert pl.available_cartridge_slots == 5

    def test_update_missing_raises(self, dao: PhysicalLibraryDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", location="x")


class TestEnableDisable:
    def test_disable(self, dao: PhysicalLibraryDaoFile):
        dao.disable("physlib_ci", reason="offline")
        pl = dao.get("physlib_ci")
        assert pl.state is PhysicalLibraryState.DISABLED
        assert pl.state_reason == "offline"

    def test_enable_clears_reason(self, dao: PhysicalLibraryDaoFile):
        dao.disable("physlib_ci", reason="offline")
        dao.enable("physlib_ci")
        pl = dao.get("physlib_ci")
        assert pl.state is PhysicalLibraryState.ENABLED
        assert pl.state_reason is None


class TestDelete:
    def test_delete_unreferenced(self, dao: PhysicalLibraryDaoFile):
        dao.create("pl_free", manufacturer="x", model="y", physical_cartridge_slots=1, physical_drive_slots=1)
        dao.delete("pl_free")
        with pytest.raises(ResourceNotFoundError):
            dao.get("pl_free")

    def test_delete_referenced_raises_backend_error(self, dao: PhysicalLibraryDaoFile):
        # physlib_ci is referenced by the seeded logical libraries.
        with pytest.raises(BackendError):
            dao.delete("physlib_ci")
