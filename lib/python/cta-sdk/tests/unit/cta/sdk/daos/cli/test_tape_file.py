from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import TapeFileDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import TapeFileDao
from cta.sdk.models import Checksum, DiskFile, TapeFile

Dao = tuple[TapeFileDao, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "tape_files.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> TapeFile:
    return TapeFile(
        archive_id=4303580667,
        storage_class_name="digital_memory",
        creation_time=datetime(2023, 2, 23, 15, 37, 50, tzinfo=timezone.utc),
        size=5,
        checksum=Checksum("ADLER32", "058801ab"),
        disk_file=DiskFile(
            id="4296353178",
            disk_instance_name="eosctapublicpps",
            uid=104039,
            gid=2766,
            path="",
        ),
        vid="I72052",
        fseq=1,
        block_id=0,
    )


class TestTapeFileDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = TapeFileDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(0, 1)}], indirect=True)
    def test_get_command(self, ref_obj: TapeFile):
        return_obj = self.manager.get(4303580667, vid="I72052")

        self.mock_run.assert_called_once_with(
            ["cta-admin", "--jsonl", "tapefile", "ls", "--id", "4303580667", "--vid", "I72052"]
        )

        return_obj = self.manager.get(4296353178, "eosctaatlaspps", vid="L85033")
        self.mock_run.assert_called_with(
            [
                "cta-admin",
                "--jsonl",
                "tapefile",
                "ls",
                "--diskfileid",
                "4296353178",
                "--instance",
                "eosctaatlaspps",
                "--vid",
                "L85033",
            ]
        )

        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(0, 2)}], indirect=True)
    def test_get_all_copies_command(self, ref_obj: TapeFile):
        return_obj = self.manager.get_all_copies(4303580667)

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "tapefile", "ls", "--id", "4303580667"])
        assert len(return_obj) == 2
        assert return_obj[0] == ref_obj

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(
            self.manager.list(
                vid="I72052",
            )
        )
        self.mock_stream_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "tapefile",
                "ls",
                "--vid",
                "I72052",
            ]
        )
        assert len(return_objs) == 3
