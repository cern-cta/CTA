# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import ActivityMountRuleDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler: CtaFileHandler) -> ActivityMountRuleDaoFile:
    return ActivityMountRuleDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao: ActivityMountRuleDaoFile):
        rule = dao.get("user_ci", "instance_ci", "act.*")
        assert rule.mount_policy_name == "mp_ci"

    def test_get_missing_raises(self, dao: ActivityMountRuleDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get("user_ci", "instance_ci", "nope.*")


class TestList:
    def test_list_contains_seed(self, dao: ActivityMountRuleDaoFile):
        assert ("user_ci", "instance_ci", "act.*") in {(r.user_name, r.disk_instance_name, r.regex) for r in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao: ActivityMountRuleDaoFile):
        dao.create("user_new", "instance_ci", "x.*", mount_policy_name="mp_ci")
        assert dao.get("user_new", "instance_ci", "x.*").mount_policy_name == "mp_ci"

    def test_create_missing_disk_instance_raises(self, dao: ActivityMountRuleDaoFile):
        with pytest.raises(BackendError):
            dao.create("user_new", "ghost", "x.*", mount_policy_name="mp_ci")

    def test_create_missing_mount_policy_raises(self, dao: ActivityMountRuleDaoFile):
        with pytest.raises(BackendError):
            dao.create("user_new", "instance_ci", "x.*", mount_policy_name="ghost")

    def test_create_duplicate_raises(self, dao: ActivityMountRuleDaoFile):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("user_ci", "instance_ci", "act.*", mount_policy_name="mp_ci")


class TestUpdate:
    def test_update_comment(self, dao: ActivityMountRuleDaoFile):
        dao.update("user_ci", "instance_ci", "act.*", comment="c2")
        assert dao.get("user_ci", "instance_ci", "act.*").metadata.comment == "c2"

    def test_update_missing_mount_policy_raises(self, dao: ActivityMountRuleDaoFile):
        with pytest.raises(BackendError):
            dao.update("user_ci", "instance_ci", "act.*", mount_policy_name="ghost")

    def test_update_missing_resource_raises(self, dao: ActivityMountRuleDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", "instance_ci", "act.*", comment="x")


class TestDelete:
    def test_delete(self, dao: ActivityMountRuleDaoFile):
        dao.delete("user_ci", "instance_ci", "act.*")
        with pytest.raises(ResourceNotFoundError):
            dao.get("user_ci", "instance_ci", "act.*")

    def test_delete_missing_raises(self, dao: ActivityMountRuleDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("user_ci", "instance_ci", "nope.*")
