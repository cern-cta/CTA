from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import VirtualOrganisationDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import Metadata, VirtualOrganisation

Dao = tuple[VirtualOrganisationDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "virtual_organisations.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> VirtualOrganisation:
    return VirtualOrganisation(
        name="ATLAS",
        disk_instance_name="eosctaatlaspps",
        is_repack_vo=False,
        max_read_drives=2,
        max_write_drives=2,
        max_file_size=128849018880,
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="eosctafst0017.cern.ch",
            creation_time=datetime(2020, 6, 29, 20, 56, 15, tzinfo=timezone.utc),
            update_user_name="idlarbi",
            update_host_name="ctapreproductionfrontend11",
            update_time=datetime(2025, 2, 13, 15, 0, 22, tzinfo=timezone.utc),
            comment="ATLAS",
        ),
    )


class TestVirtualOrganisationDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = VirtualOrganisationDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create("test_vo", disk_instance_name="test_di")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "virtualorganization",
                "add",
                "--virtualorganisation",
                "test_vo",
                "--diskinstance",
                "test_di",
                "--isrepackvo",
                "false",
                "--readmaxdrives",
                "1",
                "--writemaxdrives",
                "1",
                "--maxfilesize",
                "1000000000000",
                "--comment",
                "From python cta-sdk",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_vo")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "virtualorganization",
                "rm",
                "--virtualorganisation",
                "test_vo",
            ]
        )

    def test_update_command(self):
        self.manager.update(
            "test_vo",
            disk_instance_name="test_di",
            is_repack_vo=True,
            max_read_drives=0,
            max_write_drives=0,
            max_file_size=1_000_000_000,
            comment="New comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "virtualorganization",
                "ch",
                "--virtualorganisation",
                "test_vo",
                "--diskinstance",
                "test_di",
                "--isrepackvo",
                "true",
                "--readmaxdrives",
                "0",
                "--writemaxdrives",
                "0",
                "--maxfilesize",
                "1000000000",
                "--comment",
                "New comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: VirtualOrganisation):
        return_obj = self.manager.get("ATLAS")
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "virtualorganization", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "virtualorganization", "ls"])
        assert len(return_objs) == 3
