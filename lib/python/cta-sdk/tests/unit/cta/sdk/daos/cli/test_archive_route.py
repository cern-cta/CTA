from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import ArchiveRouteDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import ArchiveRoute, ArchiveRouteType, Metadata

Dao = tuple[ArchiveRouteDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "archive_routes.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> ArchiveRoute:
    return ArchiveRoute(
        storage_class_name="atlaspps",
        copy_number=1,
        type=ArchiveRouteType.DEFAULT,
        tape_pool_name="atlaspps",
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="eosctafst0017.cern.ch",
            creation_time=datetime(2020, 6, 29, 20, 59, 41, tzinfo=timezone.utc),
            update_user_name="jleduc",
            update_host_name="eosctafst0017.cern.ch",
            update_time=datetime(2020, 6, 29, 20, 59, 41, tzinfo=timezone.utc),
            comment="atlaspps storage class on physical tapes",
        ),
    )


class TestArchiveRouteDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = ArchiveRouteDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create("test_sc", 1, ArchiveRouteType.DEFAULT, tape_pool_name="test_tp")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "archiveroute",
                "add",
                "--storageclass",
                "test_sc",
                "--copynb",
                "1",
                "--archiveroutetype",
                "DEFAULT",
                "--tapepool",
                "test_tp",
                "--comment",
                "From python cta-sdk",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_sc", 1, ArchiveRouteType.DEFAULT)

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "archiveroute",
                "rm",
                "--storageclass",
                "test_sc",
                "--copynb",
                "1",
                "--archiveroutetype",
                "DEFAULT",
            ]
        )

    def test_update_command(self):
        self.manager.update("test_sc", 1, ArchiveRouteType.DEFAULT, tape_pool_name="new_test_tp", comment="New comment")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "archiveroute",
                "ch",
                "--storageclass",
                "test_sc",
                "--copynb",
                "1",
                "--archiveroutetype",
                "DEFAULT",
                "--tapepool",
                "new_test_tp",
                "--comment",
                "New comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: ArchiveRoute):
        return_obj = self.manager.get("atlaspps", 1, ArchiveRouteType.DEFAULT)

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "archiveroute", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "archiveroute", "ls"])
        assert len(return_objs) == 3
