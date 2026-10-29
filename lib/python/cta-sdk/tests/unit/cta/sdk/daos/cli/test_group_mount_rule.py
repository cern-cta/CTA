from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import GroupMountRuleDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import GroupMountRule, Metadata

Dao = tuple[GroupMountRuleDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "group_mount_rules.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> GroupMountRule:
    return GroupMountRule(
        group_name="ppstestgroup",
        disk_instance_name="eosctapublicpps",
        mount_policy_name="ctapublicpps",
        metadata=Metadata(
            creation_user_name="vyurchen",
            creation_host_name="eosctafst0108.cern.ch",
            creation_time=datetime(2021, 6, 16, 6, 38, 15, tzinfo=timezone.utc),
            update_user_name="vyurchen",
            update_host_name="eosctafst0108.cern.ch",
            update_time=datetime(2021, 6, 16, 6, 38, 15, tzinfo=timezone.utc),
            comment="Group for tests",
        ),
    )


class TestGroupMountRuleDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = GroupMountRuleDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_delete_command(self):
        self.manager.delete("test_gmr", "test_di")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "groupmountrule",
                "rm",
                "--name",
                "test_gmr",
                "--instance",
                "test_di",
            ]
        )

    def test_create_command(self):
        self.manager.create("test_gmr", "test_di", mount_policy_name="test_mp", comment="test_comment")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "groupmountrule",
                "add",
                "--name",
                "test_gmr",
                "--instance",
                "test_di",
                "--mountpolicy",
                "test_mp",
                "--comment",
                "test_comment",
            ]
        )

    def test_update_command(self):
        self.manager.update("test_gmr", "test_di", mount_policy_name="test_mp", comment="test_comment")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "groupmountrule",
                "ch",
                "--name",
                "test_gmr",
                "--instance",
                "test_di",
                "--mountpolicy",
                "test_mp",
                "--comment",
                "test_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: GroupMountRule):
        return_obj = self.manager.get("ppstestgroup", "eosctapublicpps")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "groupmountrule", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "groupmountrule", "ls"])
        assert len(return_objs) == 3
