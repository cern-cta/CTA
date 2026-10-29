from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import StorageClassDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import Metadata, StorageClass

Dao = tuple[StorageClassDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "storage_classes.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> StorageClass:
    return StorageClass(
        name="atlaspps",
        virtual_organisation_name="ATLAS",
        replica_count=1,
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="eosctafst0017.cern.ch",
            creation_time=datetime(2020, 6, 29, 20, 57, 13, tzinfo=timezone.utc),
            update_user_name="jleduc",
            update_host_name="eosctafst0017.cern.ch",
            update_time=datetime(2020, 6, 29, 20, 57, 13, tzinfo=timezone.utc),
            comment="atlaspps storage class on physical tapes",
        ),
    )


class TestStorageClassDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = StorageClassDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_delete_command(self):
        self.manager.delete("test_sc")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "storageclass",
                "rm",
                "--name",
                "test_sc",
            ]
        )

    def test_create_command(self):
        self.manager.create("test_sc", virtual_organisation_name="test_vo", replica_count=2, comment="test_comment")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "storageclass",
                "add",
                "--name",
                "test_sc",
                "--virtualorganisation",
                "test_vo",
                "--numberofcopies",
                "2",
                "--comment",
                "test_comment",
            ]
        )

    def test_update_command(self):
        self.manager.update("test_sc", virtual_organisation_name="test_vo", replica_count=1, comment="new_comment")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "storageclass",
                "ch",
                "--name",
                "test_sc",
                "--virtualorganisation",
                "test_vo",
                "--numberofcopies",
                "1",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(0, 1)}], indirect=True)
    def test_get_command(self, ref_obj: StorageClass):
        return_obj = self.manager.get("atlaspps")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "storageclass", "ls", "--name", "atlaspps"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "storageclass", "ls"])
        assert len(return_objs) == 3
