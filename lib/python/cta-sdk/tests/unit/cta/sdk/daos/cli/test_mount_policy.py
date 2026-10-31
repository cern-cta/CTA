from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import MountPolicyDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import Metadata, MountPolicy

Dao = tuple[MountPolicyDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "mount_policies.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> MountPolicy:
    return MountPolicy(
        name="ctaatlaspps",
        archive_priority=20,
        min_archive_age=timedelta(seconds=1200),
        retrieve_priority=10,
        min_retrieve_age=timedelta(seconds=1200),
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="eosctafst0017.cern.ch",
            creation_time=datetime(2020, 6, 29, 21, 1, 7, tzinfo=timezone.utc),
            update_user_name="jleduc",
            update_host_name="cta-frontend01",
            update_time=datetime(2023, 2, 28, 14, 40, 24, tzinfo=timezone.utc),
            comment="Mount policy for Atlas",
        ),
    )


class TestMountPolicyDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = MountPolicyDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_delete_command(self):
        self.manager.delete("test_mp")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "mountpolicy",
                "rm",
                "--name",
                "test_mp",
            ]
        )

    def test_create_command(self):
        self.manager.create(
            "test_mp",
            archive_priority=10,
            min_archive_age=timedelta(seconds=1000),
            retrieve_priority=10,
            min_retrieve_age=timedelta(seconds=2000),
            comment="test_comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "mountpolicy",
                "add",
                "--name",
                "test_mp",
                "--archivepriority",
                "10",
                "--minarchiverequestage",
                "1000",
                "--retrievepriority",
                "10",
                "--minretrieverequestage",
                "2000",
                "--comment",
                "test_comment",
            ]
        )

    def test_update_command(self):
        self.manager.update(
            "test_mp",
            archive_priority=10,
            min_archive_age=timedelta(seconds=1000),
            retrieve_priority=10,
            min_retrieve_age=timedelta(seconds=2000),
            comment="new_comment",
        )

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "mountpolicy",
                "ch",
                "--name",
                "test_mp",
                "--archivepriority",
                "10",
                "--minarchiverequestage",
                "1000",
                "--retrievepriority",
                "10",
                "--minretrieverequestage",
                "2000",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: MountPolicy):
        return_obj = self.manager.get("ctaatlaspps")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "mountpolicy", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "mountpolicy", "ls"])
        assert len(return_objs) == 3
