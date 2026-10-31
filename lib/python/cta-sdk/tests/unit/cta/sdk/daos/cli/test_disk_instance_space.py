from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import DiskInstanceSpaceDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import DiskInstanceSpace, Metadata

Dao = tuple[DiskInstanceSpaceDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "disk_instance_spaces.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> DiskInstanceSpace:
    return DiskInstanceSpace(
        name="eosctarepack_default_space",
        disk_instance_name="eosctarepack",
        free_space_url="eosSpace:default",
        refresh_interval=timedelta(seconds=10),
        free_space=145448985042944,
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="cta-frontend01",
            creation_time=datetime(2022, 4, 26, 9, 48, 25, tzinfo=timezone.utc),
            update_user_name="jleduc",
            update_host_name="cta-frontend01",
            update_time=datetime(2022, 4, 26, 9, 48, 25, tzinfo=timezone.utc),
            comment="eosctarepack repack target space",
        ),
    )


class TestDiskInstanceSpaceDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = DiskInstanceSpaceDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create(
            "test_dis",
            "test_di",
            free_space_url="eosSpace:retrieve",
            refresh_interval=timedelta(seconds=20),
            comment="test_comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "diskinstancespace",
                "add",
                "--name",
                "test_dis",
                "--diskinstance",
                "test_di",
                "--freespacequeryurl",
                "eosSpace:retrieve",
                "--refreshinterval",
                "20",
                "--comment",
                "test_comment",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_dis", "test_di")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "diskinstancespace",
                "rm",
                "--name",
                "test_dis",
                "--diskinstance",
                "test_di",
            ]
        )

    def test_update_command(self):
        self.manager.update(
            "test_dis",
            "test_di",
            free_space_url="eosSpace:changed",
            refresh_interval=timedelta(hours=1),
            comment="new_comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "diskinstancespace",
                "ch",
                "--name",
                "test_dis",
                "--diskinstance",
                "test_di",
                "--freespacequeryurl",
                "eosSpace:changed",
                "--refreshinterval",
                "3600",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: DiskInstanceSpace):
        return_obj = self.manager.get("eosctarepack_default_space", "eosctarepack")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "diskinstancespace", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "diskinstancespace", "ls"])
        assert len(return_objs) == 3
