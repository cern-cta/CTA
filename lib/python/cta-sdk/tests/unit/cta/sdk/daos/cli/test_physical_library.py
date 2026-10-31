from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import PhysicalLibraryDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import Metadata, PhysicalLibrary, PhysicalLibraryState

Dao = tuple[PhysicalLibraryDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "physical_libraries.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> PhysicalLibrary:
    return PhysicalLibrary(
        name="IBMLIB1",
        manufacturer="IBM",
        model="TS4500",
        type="LTO",
        gui_url="http://ibmlib1.cern.ch/",
        webcam_url="",
        location="513-S-034",
        physical_cartridge_slots=22356,
        available_cartridge_slots=22356,
        physical_drive_slots=48,
        state=PhysicalLibraryState.ENABLED,
        state_reason=None,
        metadata=Metadata(
            creation_user_name="vlado",
            creation_host_name="cta-frontend01",
            creation_time=datetime(2023, 7, 14, 9, 22, 20, tzinfo=timezone.utc),
            update_user_name="vlado",
            update_host_name="cta-frontend01",
            update_time=datetime(2024, 2, 1, 12, 28, 11, tzinfo=timezone.utc),
            comment="",
        ),
    )


class TestPhysicalLibraryDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = PhysicalLibraryDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create(
            "test_pl",
            manufacturer="test_manufacturer",
            model="test_model",
            physical_cartridge_slots=1000,
            physical_drive_slots=10,
        )

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "physicallibrary",
                "add",
                "--name",
                "test_pl",
                "--manufacturer",
                "test_manufacturer",
                "--model",
                "test_model",
                "--nbphysicalcartridgeslots",
                "1000",
                "--nbphysicaldriveslots",
                "10",
                "--comment",
                "From python cta-sdk",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_pl")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "physicallibrary",
                "rm",
                "--name",
                "test_pl",
            ]
        )

    def test_update_command(self):
        self.manager.update("test_pl", location="Test Building", webcam_url="http://tape-webcam.cern.ch")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "physicallibrary",
                "ch",
                "--name",
                "test_pl",
                "--webcamurl",
                "http://tape-webcam.cern.ch",
                "--location",
                "Test Building",
            ]
        )

    def test_enable_command(self):
        self.manager.enable("test_pl")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "physicallibrary",
                "ch",
                "--name",
                "test_pl",
                "--disabled",
                "false",
            ]
        )

    def test_disable_command(self):
        self.manager.disable("test_pl", reason="change_reason")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "physicallibrary",
                "ch",
                "--name",
                "test_pl",
                "--disabled",
                "true",
                "--disabledreason",
                "change_reason",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: PhysicalLibrary):
        return_obj = self.manager.get("IBMLIB1")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "physicallibrary", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list(state=PhysicalLibraryState.DISABLED))

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "physicallibrary", "ls"])
        assert len(return_objs) == 1
