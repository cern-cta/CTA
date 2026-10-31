from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import RecycleBinTapeFileDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import RecycleBinTapeFileDao
from cta.sdk.models import Checksum, DiskFile, RecycleBinTapeFile

Dao = tuple[RecycleBinTapeFileDao, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "recycle_bin_tape_files.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> RecycleBinTapeFile:
    return RecycleBinTapeFile(
        archive_id=4302486079,
        storage_class_name="ftstest_IBM1L9",
        creation_time=datetime(2022, 10, 28, 15, 39, 46, tzinfo=timezone.utc),
        size=1024000,
        checksum=Checksum("ADLER32", "1184ddbc"),
        disk_file=DiskFile(
            id="4301523000",
            disk_instance_name="eosctaatlaspps",
            uid=89952,
            gid=2766,
            path="/eos/ctaatlaspps/ftstest/IBM1L9/ftstest1/1/test01000001",
        ),
        vid="L85033",
        fseq=1000032,
        block_id=13000383,
        deletion_time=datetime(2023, 3, 26, 20, 56, 42, tzinfo=timezone.utc),
        deletion_reason="File deleted by fts3ops from the eosctaatlaspps instance",
        virtual_organisation_name="IT",
    )


class TestRecycleBinTapeFileDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = RecycleBinTapeFileDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(0, 1)}], indirect=True)
    def test_get_command(self, ref_obj: RecycleBinTapeFile):
        return_obj = self.manager.get(4302486079, vid="L85033")

        self.mock_run.assert_called_once_with(
            ["cta-admin", "--jsonl", "recycletf", "ls", "--id", "4302486079", "--vid", "L85033"]
        )

        return_obj = self.manager.get(4302486079, "eosctaatlaspps", vid="L85033")
        self.mock_run.assert_called_with(
            [
                "cta-admin",
                "--jsonl",
                "recycletf",
                "ls",
                "--diskfileid",
                "4302486079",
                "--instance",
                "eosctaatlaspps",
                "--vid",
                "L85033",
            ]
        )

        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(0, 2)}], indirect=True)
    def test_get_all_copies_command(self, ref_obj: RecycleBinTapeFile):
        return_obj = self.manager.get_all_copies(4302486079)

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "recycletf", "ls", "--id", "4302486079"])
        assert len(return_obj) == 2
        assert return_obj[0] == ref_obj

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(
            self.manager.list(
                vid="L85033",
                min_deletion_time=datetime(2000, 1, 1, tzinfo=timezone.utc),
                max_deletion_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
                virtual_organisation_name="IT",
            )
        )
        self.mock_stream_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "recycletf",
                "ls",
                "--vid",
                "L85033",
                "--virtualorganisation",
                "IT",
                "--logunixtimemin",
                "946684800",
                "--logunixtimemax",
                "1735689600",
            ]
        )
        assert len(return_objs) == 3
