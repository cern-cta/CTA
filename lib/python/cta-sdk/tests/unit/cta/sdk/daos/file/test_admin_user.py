# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import asdict
from datetime import datetime, timezone

import pytest
from cta.sdk.daos.file import AdminUserDaoFile
from cta.sdk.exceptions import ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import AdminUser, Metadata

from .conftest import SEED_CREATION_TIME


@pytest.fixture
def dao(handler) -> AdminUserDaoFile:
    return AdminUserDaoFile(handler)


@pytest.fixture
def ref_obj() -> AdminUser:
    return AdminUser(
        name="admin_ci",
        metadata=Metadata(
            creation_user_name="cta_ci",
            creation_host_name="ci-host",
            creation_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            update_user_name="cta_ci",
            update_host_name="ci-host",
            update_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            comment="Admin CI",
        ),
    )


class TestGet:
    def test_get_returns_seeded_object(self, dao, ref_obj):
        assert asdict(dao.get("admin_ci")) == asdict(ref_obj)

    def test_get_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get("nobody")


class TestList:
    def test_list_contains_seed(self, dao):
        assert [u.name for u in dao.list()] == ["admin_ci"]


class TestCreate:
    def test_create_then_get(self, dao):
        dao.create("new_admin", comment="a new admin")
        created = dao.get("new_admin")
        assert created.name == "new_admin"
        assert created.metadata.comment == "a new admin"

    def test_create_default_comment(self, dao):
        dao.create("defaulted")
        assert dao.get("defaulted").metadata.comment == "From python cta-sdk"

    def test_create_duplicate_raises(self, dao):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("admin_ci")


class TestUpdate:
    def test_update_comment(self, dao):
        dao.update("admin_ci", comment="updated")
        assert dao.get("admin_ci").metadata.comment == "updated"

    def test_update_preserves_creation_metadata(self, dao):
        dao.update("admin_ci", comment="updated")
        assert dao.get("admin_ci").metadata.creation_time == SEED_CREATION_TIME

    def test_update_none_keeps_comment(self, dao):
        dao.update("admin_ci")
        assert dao.get("admin_ci").metadata.comment == "Admin CI"

    def test_update_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.update("nobody", comment="x")


class TestDelete:
    def test_delete_removes(self, dao):
        dao.delete("admin_ci")
        assert list(dao.list()) == []

    def test_delete_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("nobody")
