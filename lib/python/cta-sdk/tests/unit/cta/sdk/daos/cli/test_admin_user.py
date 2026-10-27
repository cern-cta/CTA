from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import AdminUserDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import AdminUser, Metadata

Dao = tuple[AdminUserDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "admin_users.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> AdminUser:
    return AdminUser(
        name="afonso",
        metadata=Metadata(
            creation_user_name="rbachman",
            creation_host_name="cta-frontend01",
            creation_time=datetime(2022, 7, 4, 8, 14, 22, tzinfo=timezone.utc),
            update_user_name="rbachman",
            update_host_name="cta-frontend01",
            update_time=datetime(2022, 7, 4, 8, 14, 22, tzinfo=timezone.utc),
            comment="Joao Afonso",
        ),
    )


class TestAdminUserDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = AdminUserDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create("test_user", comment="Test User")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "admin",
                "add",
                "--username",
                "test_user",
                "--comment",
                "Test User",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_user")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "admin",
                "rm",
                "--username",
                "test_user",
            ]
        )

    def test_update_command(self):
        self.manager.update("test_user", comment="New comment")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "admin",
                "ch",
                "--username",
                "test_user",
                "--comment",
                "New comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj):
        return_obj = self.manager.get("afonso")
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "admin", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "admin", "ls"])
        assert len(return_objs) == 3
