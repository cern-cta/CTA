from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import RepackRequestDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import (
    RepackRequest,
    RepackStartInfo,
    RepackState,
    RepackStatistics,
)

Dao = tuple[RepackRequestDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "repacks.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> RepackRequest:
    return RepackRequest(
        vid="I71576",
        tape_pool_name="test_IT_rucio",
        state=RepackState.FAILED,
        failed_file_count=7,
        disk_buffer_url="root://eosctarepack//eos/ctarepack/tmp",
        duration=timedelta(seconds=2959),
        finished_time=datetime(2025, 10, 18, 13, 35, 27, tzinfo=timezone.utc),
        destination_infos=[],
        last_expanded_fseq=57,
        start_info=RepackStartInfo(
            file_count=7,
            byte_count=15180950480,
            creation_user_name="vlado",
            creation_host_name="ctapreproductionfrontend11",
            creation_time=datetime(2025, 10, 18, 12, 46, 8, tzinfo=timezone.utc),
            is_all_files_selected_at_start=True,
        ),
        retrieve_statistics=RepackStatistics(
            total_file_count=7,
            total_byte_count=15180950480,
            transferred_file_count=0,
            transferred_byte_count=0,
            failed_file_count=7,
            failed_byte_count=15180950480,
        ),
        archive_statistics=RepackStatistics(
            total_file_count=7,
            total_byte_count=15180950480,
            transferred_file_count=0,
            transferred_byte_count=0,
            failed_file_count=0,
            failed_byte_count=0,
        ),
    )


class TestRepackRequestDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = RepackRequestDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create(
            "test_vid", mount_policy_name="test_mp", no_recall=True, max_files_to_select=10, custom_operation="MOVE"
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "repack",
                "add",
                "--vid",
                "test_vid",
                "--mountpolicy",
                "test_mp",
                "--maxfilestoselect",
                "10",
                "--no-recall",
                "--justmove",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_vid")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "repack",
                "rm",
                "--vid",
                "test_vid",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(1)}], indirect=True)
    def test_get_command(self, ref_obj):
        return_obj = self.manager.get("test_vid")
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "repack", "ls", "--vid", "test_vid"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "repack", "ls"])
        assert len(return_objs) == 2
