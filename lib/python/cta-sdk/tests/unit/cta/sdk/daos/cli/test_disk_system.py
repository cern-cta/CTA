from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import DiskSystemDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import DiskSystem, Metadata

Dao = tuple[DiskSystemDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "disk_systems.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> DiskSystem:
    return DiskSystem(
        name="ftstest_atlaspps_retrieve",
        disk_instance_name="eosctaatlaspps",
        disk_instance_space_name="eosctaatlaspps_default_space",
        file_regex=r"/eos/ctaatlaspps/ftstest/.*",
        desired_min_free_space=1000000000000,
        sleep_time=timedelta(hours=1),
        metadata=Metadata(
            creation_user_name="vyurchen",
            creation_host_name="cta-frontend01",
            creation_time=datetime(2022, 10, 24, 9, 32, 39, tzinfo=timezone.utc),
            update_user_name="vyurchen",
            update_host_name="cta-frontend01",
            update_time=datetime(2022, 10, 24, 9, 32, 39, tzinfo=timezone.utc),
            comment="free space for ftstest on eosctaatlaspps",
        ),
    )


class TestDiskSystemDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = DiskSystemDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create(
            "test_ds",
            disk_instance_name="test_di",
            disk_instance_space_name="test_dis",
            file_regex=r".*",
            desired_min_free_space=1_000_000_000,
            sleep_time=timedelta(hours=1),
            comment="test_comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "disksystem",
                "add",
                "--disksystem",
                "test_ds",
                "--diskinstance",
                "test_di",
                "--diskinstancespace",
                "test_dis",
                "--fileregexp",
                ".*",
                "--targetedfreespace",
                "1000000000",
                "--sleeptime",
                "3600",
                "--comment",
                "test_comment",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_ds")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "disksystem",
                "rm",
                "--disksystem",
                "test_ds",
            ]
        )

    def test_update_command(self):
        self.manager.update(
            "test_ds",
            sleep_time=13,
            comment="new_comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "disksystem",
                "ch",
                "--disksystem",
                "test_ds",
                "--sleeptime",
                "13",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: DiskSystem):
        return_obj = self.manager.get(
            "ftstest_atlaspps_retrieve",
        )

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "disksystem", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "disksystem", "ls"])
        assert len(return_objs) == 3
