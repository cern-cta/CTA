# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import DriveDaoFile
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import DriveOperatingState


@pytest.fixture
def dao(handler) -> DriveDaoFile:
    return DriveDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao):
        drive = dao.get("drive_ci")
        assert drive.logical_library_name == "loglib_ci"
        assert drive.state is DriveOperatingState.DOWN

    def test_get_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao):
        assert {d.name for d in dao.list()} == {"drive_ci"}

    def test_list_all_flag(self, dao):
        assert {d.name for d in dao.list(all=True)} == {"drive_ci"}


class TestUpdate:
    def test_update_comment(self, dao):
        dao.update("drive_ci", comment="new drive comment")
        assert dao.get("drive_ci").metadata.comment == "new drive comment"

    def test_update_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", comment="x")


class TestDownUp:
    def test_down_sets_state_and_reason(self, dao):
        dao.down("drive_ci", reason="broken belt")
        drive = dao.get("drive_ci")
        assert drive.state is DriveOperatingState.DOWN
        assert drive.desired_state is DriveOperatingState.DOWN
        assert drive.state_reason == "broken belt"

    def test_up_clears_reason(self, dao):
        dao.down("drive_ci", reason="broken belt")
        dao.up("drive_ci")
        drive = dao.get("drive_ci")
        assert drive.state is DriveOperatingState.UP
        assert drive.desired_state is DriveOperatingState.UP
        assert drive.state_reason == ""

    def test_up_with_reason(self, dao):
        dao.up("drive_ci", reason="fixed")
        assert dao.get("drive_ci").state_reason == "fixed"


class TestDelete:
    def test_delete(self, dao):
        dao.delete("drive_ci")
        with pytest.raises(ResourceNotFoundError):
            dao.get("drive_ci")

    def test_delete_force(self, dao):
        dao.delete("drive_ci", force=True)
        assert list(dao.list()) == []

    def test_delete_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("ghost")
