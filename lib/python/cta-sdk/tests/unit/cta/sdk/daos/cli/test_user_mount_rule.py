from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import UserMountRuleDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import Metadata, UserMountRule

Dao = tuple[UserMountRuleDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "user_mount_rules.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> UserMountRule:
    return UserMountRule(
        user_name="ams",
        disk_instance_name="eosctaatlaspps",
        mount_policy_name="rucioit",
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="ctapreproductionfrontend11",
            creation_time=datetime(2025, 5, 5, 8, 24, 41, tzinfo=timezone.utc),
            update_user_name="jleduc",
            update_host_name="ctapreproductionfrontend11",
            update_time=datetime(2025, 5, 5, 9, 45, 9, tzinfo=timezone.utc),
            comment="AMS for rucio IT Service service tests (operations #1644)",
        ),
    )


class TestUserMountRuleDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = UserMountRuleDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_delete_command(self):
        self.manager.delete("test_umr", "test_di")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "requestermountrule",
                "rm",
                "--name",
                "test_umr",
                "--instance",
                "test_di",
            ]
        )

    def test_create_command(self):
        self.manager.create("test_umr", "test_di", mount_policy_name="test_mp", comment="test_comment")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "requestermountrule",
                "add",
                "--name",
                "test_umr",
                "--instance",
                "test_di",
                "--mountpolicy",
                "test_mp",
                "--comment",
                "test_comment",
            ]
        )

    def test_update_command(self):
        self.manager.update("test_umr", "test_di", mount_policy_name="test_mp", comment="test_comment")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "requestermountrule",
                "ch",
                "--name",
                "test_umr",
                "--instance",
                "test_di",
                "--mountpolicy",
                "test_mp",
                "--comment",
                "test_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: UserMountRule):
        return_obj = self.manager.get("ams", "eosctaatlaspps")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "requestermountrule", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "requestermountrule", "ls"])
        assert len(return_objs) == 3
