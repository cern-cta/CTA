# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import LogicalLibraryDaoFile
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import LogicalLibraryState


@pytest.fixture
def dao(handler) -> LogicalLibraryDaoFile:
    return LogicalLibraryDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao):
        ll = dao.get("loglib_ci")
        assert ll.physical_library_name == "physlib_ci"
        assert ll.state is LogicalLibraryState.ENABLED

    def test_get_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_all(self, dao):
        assert {"loglib_ci", "loglib_disabled"} <= {ll.name for ll in dao.list()}

    def test_list_filter_enabled(self, dao):
        names = {ll.name for ll in dao.list(state=LogicalLibraryState.ENABLED)}
        assert "loglib_ci" in names
        assert "loglib_disabled" not in names

    def test_list_filter_disabled(self, dao):
        names = {ll.name for ll in dao.list(state=LogicalLibraryState.DISABLED)}
        assert names == {"loglib_disabled"}


class TestCreate:
    def test_create_defaults_enabled(self, dao):
        dao.create("ll_new", physical_library_name="physlib_ci")
        ll = dao.get("ll_new")
        assert ll.state is LogicalLibraryState.ENABLED
        assert ll.state_reason is None

    def test_create_missing_physical_library_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create("ll_bad", physical_library_name="ghost")

    def test_create_duplicate_raises(self, dao):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("loglib_ci", physical_library_name="physlib_ci")


class TestUpdate:
    def test_update_comment(self, dao):
        dao.update("loglib_ci", comment="c2")
        assert dao.get("loglib_ci").metadata.comment == "c2"

    def test_update_missing_physical_library_raises(self, dao):
        with pytest.raises(BackendError):
            dao.update("loglib_ci", physical_library_name="ghost")

    def test_update_missing_resource_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", comment="x")


class TestEnableDisable:
    def test_disable_sets_state_and_reason(self, dao):
        dao.disable("loglib_ci", reason="maintenance")
        ll = dao.get("loglib_ci")
        assert ll.state is LogicalLibraryState.DISABLED
        assert ll.state_reason == "maintenance"

    def test_enable_clears_reason(self, dao):
        dao.enable("loglib_disabled")
        ll = dao.get("loglib_disabled")
        assert ll.state is LogicalLibraryState.ENABLED
        assert ll.state_reason is None


class TestDelete:
    def test_delete_unreferenced(self, dao):
        dao.delete("loglib_disabled")
        with pytest.raises(ResourceNotFoundError):
            dao.get("loglib_disabled")

    def test_delete_referenced_raises_backend_error(self, dao):
        # loglib_ci is referenced by the seeded tapes.
        with pytest.raises(BackendError):
            dao.delete("loglib_ci")
