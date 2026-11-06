# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cta_admin import DiskInstanceDaoCtaAdmin
from cta.sdk.daos.cta_admin._wrapper import CtaAdminWrapper
from cta.sdk.models import DiskInstance, Metadata

Dao = tuple[DiskInstanceDaoCtaAdmin, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "disk_instances.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> DiskInstance:
    return DiskInstance(
        name="eosctaatlaspps",
        metadata=Metadata(
            creation_user_name="mvelosob",
            creation_host_name="ctapreproductionfrontend01",
            creation_time=datetime(2022, 4, 27, 13, 10, 9, tzinfo=timezone.utc),
            update_user_name="mvelosob",
            update_host_name="ctapreproductionfrontend01",
            update_time=datetime(2022, 4, 27, 13, 10, 9, tzinfo=timezone.utc),
            comment="eosctaatlaspps instance for existing archive file entries",
        ),
    )


class TestDiskInstanceDaoCtaAdmin:
    @pytest.fixture(autouse=True)
    def set_cta_admin_wrapper(self, mock_wrapper: tuple[CtaAdminWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = DiskInstanceDaoCtaAdmin(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create(
            "test_di",
            comment="test_comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "diskinstance",
                "add",
                "--name",
                "test_di",
                "--comment",
                "test_comment",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_di")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "diskinstance",
                "rm",
                "--name",
                "test_di",
            ]
        )

    def test_update_command(self):
        self.manager.update(
            "test_di",
            comment="new_comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "diskinstance",
                "ch",
                "--name",
                "test_di",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: DiskInstance):
        return_obj = self.manager.get(
            "eosctaatlaspps",
        )

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "diskinstance", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "diskinstance", "ls"])
        assert len(return_objs) == 3
