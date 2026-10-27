# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import GroupMountRuleDaoFile
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler) -> GroupMountRuleDaoFile:
    return GroupMountRuleDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao):
        assert dao.get("group_ci", "instance_ci").mount_policy_name == "mp_ci"

    def test_get_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost", "instance_ci")


class TestList:
    def test_list_contains_seed(self, dao):
        assert ("group_ci", "instance_ci") in {(r.group_name, r.disk_instance_name) for r in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao):
        dao.create("group_new", "instance_ci", mount_policy_name="mp_ci")
        assert dao.get("group_new", "instance_ci").mount_policy_name == "mp_ci"

    def test_create_missing_disk_instance_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create("group_bad", "ghost", mount_policy_name="mp_ci")

    def test_create_missing_mount_policy_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create("group_bad", "instance_ci", mount_policy_name="ghost")

    def test_create_duplicate_raises(self, dao):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("group_ci", "instance_ci", mount_policy_name="mp_ci")


class TestUpdate:
    def test_update_comment(self, dao):
        dao.update("group_ci", "instance_ci", comment="c2")
        assert dao.get("group_ci", "instance_ci").metadata.comment == "c2"

    def test_update_missing_mount_policy_raises(self, dao):
        with pytest.raises(BackendError):
            dao.update("group_ci", "instance_ci", mount_policy_name="ghost")

    def test_update_missing_resource_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", "instance_ci", comment="x")


class TestDelete:
    def test_delete(self, dao):
        dao.delete("group_ci", "instance_ci")
        with pytest.raises(ResourceNotFoundError):
            dao.get("group_ci", "instance_ci")

    def test_delete_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("ghost", "instance_ci")
