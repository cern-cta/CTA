from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import LogicalLibraryDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import LogicalLibrary, LogicalLibraryState, Metadata

Dao = tuple[LogicalLibraryDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "logical_libraries.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> LogicalLibrary:
    return LogicalLibrary(
        name="IBM1L9",
        physical_library_name="IBMLIB1",
        state=LogicalLibraryState.ENABLED,
        state_reason=None,
        metadata=Metadata(
            creation_user_name="CASTOR",
            creation_host_name="CASTOR",
            creation_time=datetime(2021, 8, 27, 9, 52, 11, tzinfo=timezone.utc),
            update_user_name="idlarbi",
            update_host_name="ctapreproductionfrontend11",
            update_time=datetime(2025, 7, 11, 8, 57, 43, tzinfo=timezone.utc),
            comment="IBMLIB1 (IBM TS4500 tape library in B513) - LTO-9 tape drives",
        ),
    )


class TestLogicalLibraryDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = LogicalLibraryDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create("test_ll", physical_library_name="test_pl", state=LogicalLibraryState.DISABLED)

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "logicallibrary",
                "add",
                "--name",
                "test_ll",
                "--physicallibrary",
                "test_pl",
                "--disabled",
                "true",
                "--comment",
                "From python cta-sdk",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_ll")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "logicallibrary",
                "rm",
                "--name",
                "test_ll",
            ]
        )

    def test_update_command(self):
        self.manager.update("test_ll", physical_library_name="test_pl")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "logicallibrary",
                "ch",
                "--name",
                "test_ll",
                "--physicallibrary",
                "test_pl",
            ]
        )

    def test_enable_command(self):
        self.manager.enable("test_ll")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "logicallibrary",
                "ch",
                "--name",
                "test_ll",
                "--disabled",
                "false",
            ]
        )

    def test_disable_command(self):
        self.manager.disable("test_ll", reason="change_reason")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "logicallibrary",
                "ch",
                "--name",
                "test_ll",
                "--disabled",
                "true",
                "--disabledreason",
                "change_reason",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: LogicalLibrary):
        return_obj = self.manager.get("IBM1L9")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "logicallibrary", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(2)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list(state=LogicalLibraryState.ENABLED))

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "logicallibrary", "ls", "--disabled", "false"])
        assert len(return_objs) == 2
