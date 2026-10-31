from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import UserRequestDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import MountType, UserRequest, UserRequestSummary

Dao = tuple[UserRequestDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "requests.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> UserRequest:
    return UserRequest(
        mount_type=MountType.ARCHIVE,
        tape_pool_name="backup_HDFS",
        vid=None,
        virtual_organisation_name="BACKUP_HDFS",
        logical_library_name="",
        queued_file_count=9,
        queued_byte_count=13719009715,
        oldest_age=timedelta(seconds=1563),
        priority=250,
        min_age=timedelta(seconds=14400),
        mount_count=0,
        transferred_file_count=0,
        transferred_byte_count=0,
        is_sleeping_for_space=False,
        sleep_start_time=None,
        scheduler_backend_name="cephUser",
        tapes_byte_count=121932120767281,
        tapes_file_count=3618,
        tapes_capacity=121000000000000,
    )


class TestRequestDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = UserRequestDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self, ref_obj: UserRequest):
        return_objs = list(self.manager.list(request_type=MountType.ARCHIVE))
        self.mock_stream_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "showqueues",
            ]
        )
        assert len(return_objs) == 3
        assert asdict(return_objs[0]) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_summary_command(self):
        return_objs = self.manager.list(request_type=MountType.ARCHIVE)

        return_obj = UserRequestDaoCli.summary(list(return_objs), "tape_pool_name")
        assert return_obj[1] == UserRequestSummary.group_by("tape_pool_name")(
            "backup_HDFS",
            timedelta(seconds=14400),
            timedelta(seconds=7945),
            11,
            13719010203,
            0,
            0,
            0,
            None,
            8322890,
            18679761883902038,
            12397000000000000,
            250,
        )
