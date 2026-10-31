from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import ActivityMountRuleDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import ActivityMountRule, Metadata


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "activity_mount_rules.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> ActivityMountRule:
    return ActivityMountRule(
        user_name="ctaops",
        disk_instance_name="eosctaatlaspps",
        regex=r"^Julien is Staging$",
        mount_policy_name="express",
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="ctapreproductionfrontend11",
            creation_time=datetime(2025, 4, 8, 11, 36, 43, tzinfo=timezone.utc),
            update_user_name="jleduc",
            update_host_name="ctapreproductionfrontend11",
            update_time=datetime(2025, 4, 8, 11, 36, 43, tzinfo=timezone.utc),
            comment="preproduction activity with spaces and case mix",
        ),
    )


class TestActivityMountRuleDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = ActivityMountRuleDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create("ctaops", "eosctaatlaspps", r".*", mount_policy_name="express")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "activitymountrule",
                "add",
                "--name",
                "ctaops",
                "--instance",
                "eosctaatlaspps",
                "--activityregex",
                ".*",
                "--mountpolicy",
                "express",
                "--comment",
                "From python cta-sdk",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("ctaops", "eosctaatlaspps", r".*")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "activitymountrule",
                "rm",
                "--name",
                "ctaops",
                "--instance",
                "eosctaatlaspps",
                "--activityregex",
                ".*",
            ]
        )

    def test_update_command(self):
        self.manager.update("ctaops", "eosctaatlaspps", r".*", mount_policy_name="new_mp", comment="New comment")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "activitymountrule",
                "ch",
                "--name",
                "ctaops",
                "--instance",
                "eosctaatlaspps",
                "--activityregex",
                ".*",
                "--mountpolicy",
                "new_mp",
                "--comment",
                "New comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: ActivityMountRule):
        return_obj = self.manager.get("ctaops", "eosctaatlaspps", r"^Julien is Staging$")
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "activitymountrule", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "activitymountrule", "ls"])
        assert len(return_objs) == 3
