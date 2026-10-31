from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import TapePoolDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import Metadata, TapePool, TapePoolStatistics

Dao = tuple[TapePoolDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "tape_pools.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> TapePool:
    return TapePool(
        name="atlaspps",
        virtual_organisation_name="ATLAS",
        min_writable_tape_count=1,
        encryption_key_name=None,
        supply_tape_pool_names=["supply_alltypes"],
        statistics=TapePoolStatistics(tape_count=1, file_count=864, capacity=18000000000000, usage=1586358122465),
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="eosctafst0017.cern.ch",
            creation_time=datetime(2020, 6, 29, 20, 58, 6, tzinfo=timezone.utc),
            update_user_name="vlado",
            update_host_name="ctapreproductionfrontend11",
            update_time=datetime(2025, 5, 7, 10, 3, 11, tzinfo=timezone.utc),
            comment="tape pool for atlaspps",
        ),
    )


class TestTapePoolDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = TapePoolDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create("test_tp", virtual_organisation_name="test_vo")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "tapepool",
                "add",
                "--name",
                "test_tp",
                "--virtualorganisation",
                "test_vo",
                "--partialtapesnumber",
                "1",
                "--comment",
                "From python cta-sdk",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_tp")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "tapepool",
                "rm",
                "--name",
                "test_tp",
            ]
        )

    def test_update_command(self):
        self.manager.update(
            "test_tp",
            virtual_organisation_name="test_vo",
            min_writable_tape_count=3,
            supply_tape_pool_names=["a", "b", "c"],
            comment="new_comment",
        )

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "tapepool",
                "ch",
                "--name",
                "test_tp",
                "--virtualorganisation",
                "test_vo",
                "--partialtapesnumber",
                "3",
                "--supply",
                "a,b,c",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(1)}], indirect=True)
    def test_get_command(self, ref_obj: TapePool):
        return_obj = self.manager.get("atlaspps")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "tapepool", "ls", "--name", "atlaspps"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(1, 2)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list(encrypted=False, virtual_organisation_name="IT"))

        self.mock_run.assert_called_once_with(
            ["cta-admin", "--jsonl", "tapepool", "ls", "--virtualorganisation", "IT", "--encrypted", "false"]
        )
        assert len(return_objs) == 1
