from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import TapeDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import Tape, TapeMetadata, TapeState, TapeStatistics, VerificationStatus

Dao = tuple[TapeDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "tapes.jsonl").read_text()


@pytest.fixture
def manager(mock_wrapper):
    return TapeDaoCli(mock_wrapper[0]), mock_wrapper[1]


@pytest.fixture(scope="module")
def ref_obj() -> Tape:
    return Tape(
        vid="I71007",
        media_type_name="3592JE20T",
        vendor="FUJIFILM",
        purchase_order="",
        logical_library_name="IBMLIB3-TS1160",
        physical_library_name="IBMLIB3",
        tape_pool_name="supply_alltypes",
        virtual_organisation_name="IT",
        encryption_key_name=None,
        capacity=20000000000000,
        format="CTA",
        state=TapeState.ACTIVE,
        state_reason="",
        verification_status=VerificationStatus(
            date=datetime(2022, 9, 4, 10, 00, 45, tzinfo=timezone.utc),
            status="finished",
            file_submitted_count=2624,
            file_verified_count=0,
            file_failed_count=0,
        ),
        statistics=TapeStatistics(
            physical_usage=0,
            logical_usage=0,
            is_dirty=False,
            last_fseq=0,
            is_full=False,
            file_count=0,
        ),
        metadata=TapeMetadata(
            creation_user_name="vlado",
            creation_host_name="cta-frontend01",
            creation_time=datetime(2022, 4, 14, 7, 52, 20, tzinfo=timezone.utc),
            update_user_name="vlado",
            update_host_name="ctapreproductionfrontend11",
            update_time=datetime(2025, 10, 20, 9, 48, 25, tzinfo=timezone.utc),
            comment="",
            label_drive_name="IBMLIB3-TS1160-F14C1R2",
            label_time=datetime(2025, 10, 20, 9, 48, 24, tzinfo=timezone.utc),
            last_write_drive_name="IBMLIB3-TS1160-F14C1R2",
            last_write_time=datetime(2025, 2, 12, 8, 45, 49, tzinfo=timezone.utc),
            last_read_drive_name="IBMLIB4-TS1160-F08C2R2",
            last_read_time=datetime(2025, 3, 19, 17, 3, 34, tzinfo=timezone.utc),
            update_state_info="vlado@cta-frontend01",
            update_state_time=datetime(2022, 4, 14, 7, 52, 20, tzinfo=timezone.utc),
            write_mount_count=99,
            read_mount_count=0,
        ),
    )


class TestTapeDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = TapeDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create(
            "test_vid",
            media_type_name="test_mt",
            vendor="test_vendor",
            logical_library_name="test_ll",
            tape_pool_name="test_tp",
        )

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "tape",
                "add",
                "--vid",
                "test_vid",
                "--mediatype",
                "test_mt",
                "--vendor",
                "test_vendor",
                "--logicallibrary",
                "test_ll",
                "--tapepool",
                "test_tp",
                "--full",
                "false",
                "--comment",
                "From python cta-sdk",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_vid")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "tape",
                "rm",
                "--vid",
                "test_vid",
            ]
        )

    def test_update_command(self):
        self.manager.update(
            "test_vid",
            media_type_name="test_mt",
            vendor="test_vendor",
            logical_library_name="test_ll",
            tape_pool_name="test_tp",
            verification_status=VerificationStatus(
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                status="finished",
                file_submitted_count=10,
                file_verified_count=0,
                file_failed_count=0,
            ),
            dirty=False,
            comment="new_comment",
        )

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "tape",
                "ch",
                "--vid",
                "test_vid",
                "--mediatype",
                "test_mt",
                "--vendor",
                "test_vendor",
                "--logicallibrary",
                "test_ll",
                "--tapepool",
                "test_tp",
                "--verificationstatus",
                '{"date": 1735689600, "status": "finished", "files_submitted": 10, "files_verified": 0, "files_failed": 0}',
                "--dirtybit",
                "false",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(1)}], indirect=True)
    def test_get_command(self, ref_obj: Tape):
        return_obj = self.manager.get("I71007")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "tape", "ls", "--vid", "I71007"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list(full=False, virtual_organisation_name="IT"))

        self.mock_stream_run.assert_called_once_with(
            ["cta-admin", "--jsonl", "tape", "ls", "--virtualorganisation", "IT", "--full", "false"]
        )
        assert len(return_objs) == 3

    def test_change_state(self):
        self.manager.change_state("test_vid", state=TapeState.BROKEN, reason="state_reason")
        self.mock_run.assert_called_once_with(
            ["cta-admin", "--jsonl", "tape", "ch", "--vid", "test_vid", "--state", "BROKEN", "--reason", "state_reason"]
        )

    def test_reclaim_command(self):
        self.manager.reclaim("test_vid")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "tape",
                "reclaim",
                "--vid",
                "test_vid",
            ]
        )
