# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta
from pathlib import Path

import pytest
from cta.sdk.client.file import CtaClientFile
from cta.sdk.daos.file import DiskSystemDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import BackendError, ResourceNotFoundError


@pytest.fixture
def dao(handler: CtaFileHandler) -> DiskSystemDaoFile:
    return DiskSystemDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao: DiskSystemDaoFile):
        ds = dao.get("system_ci")
        assert ds.disk_instance_name == "instance_ci"
        assert ds.disk_instance_space_name == "space_ci"
        assert ds.sleep_time == timedelta(hours=1)

    def test_get_missing_raises(self, dao: DiskSystemDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_contains_seed(self, dao: DiskSystemDaoFile):
        assert "system_ci" in {ds.name for ds in dao.list()}


class TestCreate:
    def test_create_then_get(self, dao: DiskSystemDaoFile):
        dao.create(
            "system_new",
            disk_instance_name="instance_ci",
            disk_instance_space_name="space_ci",
            file_regex=".*",
            sleep_time=120,
        )
        assert dao.get("system_new").sleep_time == timedelta(seconds=120)

    def test_create_missing_space_raises(self, dao: DiskSystemDaoFile):
        with pytest.raises(BackendError):
            dao.create(
                "system_bad",
                disk_instance_name="instance_ci",
                disk_instance_space_name="ghost",
                file_regex=".*",
            )


class TestUpdate:
    def test_update_fields(self, dao: DiskSystemDaoFile):
        dao.update("system_ci", file_regex="/new/.*", desired_min_free_space=42, sleep_time=10)
        ds = dao.get("system_ci")
        assert ds.file_regex == "/new/.*"
        assert ds.desired_min_free_space == 42
        assert ds.sleep_time == timedelta(seconds=10)

    def test_update_missing_raises(self, dao: DiskSystemDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", file_regex="x")


class TestDelete:
    def test_delete(self, dao: DiskSystemDaoFile):
        dao.delete("system_ci")
        with pytest.raises(ResourceNotFoundError):
            dao.get("system_ci")

    def test_delete_missing_raises(self, dao: DiskSystemDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("ghost")


def test_file_client_uses_disk_system_dao(data_root: Path) -> None:
    client = CtaClientFile(str(data_root))
    assert isinstance(client.disk_system, DiskSystemDaoFile)
    assert client.disk_system.get("system_ci").disk_instance_space_name == "space_ci"
    assert client.disk_system is client.disk_system
