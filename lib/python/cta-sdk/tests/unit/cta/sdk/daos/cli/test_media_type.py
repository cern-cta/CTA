from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import MediaTypeDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import MediaType, Metadata

Dao = tuple[MediaTypeDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "media_types.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> MediaType:
    return MediaType(
        name="3592JC7T",
        cartridge_type="3592JC",
        capacity=7_000_000_000_000,
        primary_density_code=84,
        secondary_density_code=0,
        metadata=Metadata(
            creation_user_name="jleduc",
            creation_host_name="cta-frontend01.cern.ch",
            creation_time=datetime(2020, 6, 29, 21, 8, 22, tzinfo=timezone.utc),
            update_user_name="jleduc",
            update_host_name="cta-frontend01.cern.ch",
            update_time=datetime(2020, 6, 29, 21, 8, 22, tzinfo=timezone.utc),
            comment="IBM 3592JC cartridge formated at 7 TB",
        ),
    )


class TestMediaTypeDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = MediaTypeDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_create_command(self):
        self.manager.create("test_mt", cartridge_type="test_type", capacity=10)

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "mediatype",
                "add",
                "--name",
                "test_mt",
                "--cartridge",
                "test_type",
                "--capacity",
                "10",
                "--primarydensitycode",
                "0",
                "--secondarydensitycode",
                "0",
                "--comment",
                "From python cta-sdk",
            ]
        )

    def test_delete_command(self):
        self.manager.delete("test_mt")

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "mediatype",
                "rm",
                "--name",
                "test_mt",
            ]
        )

    def test_update_command(self):
        self.manager.update(
            "test_mt",
            cartridge_type="test_type",
            primary_density_code=20,
            secondary_density_code=20,
            comment="new_comment",
        )

        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "mediatype",
                "ch",
                "--name",
                "test_mt",
                "--cartridge",
                "test_type",
                "--primarydensitycode",
                "20",
                "--secondarydensitycode",
                "20",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_get_command(self, ref_obj: MediaType):
        return_obj = self.manager.get("3592JC7T")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "mediatype", "ls"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "mediatype", "ls"])
        assert len(return_objs) == 3
