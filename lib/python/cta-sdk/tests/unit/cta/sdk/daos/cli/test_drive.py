from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import DriveDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import (
    Drive,
    DriveConfig,
    DriveMetadata,
    DriveOperatingState,
    MountType,
    TapeSession,
    TapeSessionState,
)

Dao = tuple[DriveDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "drives.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> Drive:
    return Drive(
        name="SPECTRALIB3-LTO9-F10B1S1",
        logical_library_name="SPECTRALIB3-LTO9",
        host_name="tpsrv640",
        desired_state=DriveOperatingState.UP,
        state=DriveOperatingState.UP,
        state_reason="",
        device_path="/dev/tape/by-name/SPECTRALIB3-LTO9-F10B1S1",
        physical_library_name="SPECTRALIB3",
        physical_library_slot="smc3",
        tape_session=TapeSession(
            id=2089339,
            vid="L96884",
            tape_pool_name="vo_NA62_2",
            mount_type=MountType.RETRIEVE,
            file_transferred_count=54,
            byte_transferred_count=318701085660,
            virtual_organisation_name="NA62",
            state=TapeSessionState.TRANSFERRING,
            duration=timedelta(seconds=832),
        ),
        state_change_age=timedelta(seconds=832),
        config=DriveConfig(
            {
                "taped.DaemonUserName": "cta",
                "taped.DaemonGroupName": "tape",
                "taped.LogMask": "INFO",
                "taped.BufferSizeBytes": "5242880",
                "taped.BufferCount": "5000",
                "taped.ArchiveFetchFiles": "4000",
                "taped.ArchiveFetchBytes": "80000000000",
                "taped.ArchiveFlushFiles": "200",
                "taped.ArchiveFlushBytes": "32000000000",
                "taped.RetrieveFetchFiles": "4000",
                "taped.RetrieveFetchBytes": "80000000000",
                "taped.MountCriteriaFiles": "100000",
                "taped.MountCriteriaBytes": "500000000000",
                "taped.NbDiskThreads": "10",
                "taped.UseRAO": "yes",
                "taped.RAOLTOAlgorithm": "sltf",
                "taped.UseEncryption": "yes",
                "taped.externalEncryptionKeyScript": "/usr/local/bin/cta-get-encryption-key.py",
                "taped.RAOLTOAlgorithmOptions": "cost_heuristic_name:cta",
                "taped.WatchdogScheduleMaxSecs": "300",
                "taped.WatchdogMountMaxSecs": "600",
                "taped.WatchdogNoBlockMoveMaxSecs": "600",
                "taped.WatchdogIdleSessionTimer": "10",
                "ObjectStore.BackendPath": "rados://cta-production@tapecta:cta-production",
                "taped.CatalogueConfigFile": "/etc/cta/cta-catalogue.conf",
                "environment.XrdSecPROTOCOL": "sss",
                "environment.XrdSecSSSKT": "/etc/cta/cta-taped.sss.keytab",
                "taped.UseRepackManagement": "yes",
                "taped.UseMaintenanceProcess": "yes",
                "taped.RepackMaxRequestsToExpand": "2",
                "taped.externalFreeDiskSpaceScript": "/usr/bin/cta-eosdf.sh",
                "taped.TapeLoadTimeout": "300",
                "taped.WatchdogGetNextMountMaxSecs": "900",
                "general.SchedulerBackendName": "cephUser",
            }
        ),
        metadata=DriveMetadata(
            last_refresh_age=timedelta(seconds=14),
            cta_version="5.11.12.1-1",
            scheduler_namespace="cephUser",
            comment="New Drive",
        ),
    )


class TestDriveDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = DriveDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_delete_command(self):
        self.manager.delete("test_drive")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "drive",
                "rm",
                "test_drive",
            ]
        )

    def test_up_command(self):
        self.manager.up("test_drive")
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "drive", "up", "test_drive"])

    def test_down_command(self):
        self.manager.down("test_drive", reason="test_reason", force=True)
        self.mock_run.assert_called_once_with(
            ["cta-admin", "--jsonl", "drive", "down", "test_drive", "--reason", "test_reason", "--force"]
        )

    def test_update_command(self):
        self.manager.update(
            "test_drive",
            comment="new_comment",
        )
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "drive",
                "ch",
                "test_drive",
                "--comment",
                "new_comment",
            ]
        )

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(0, 1)}], indirect=True)
    def test_get_command(self, ref_obj: Drive):
        return_obj = self.manager.get("SPECTRALIB3-LTO9-F10B1S1")

        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "drive", "ls", "SPECTRALIB3-LTO9-F10B1S1"])
        assert asdict(return_obj) == asdict(ref_obj)

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self):
        return_objs = list(self.manager.list())
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "drive", "ls"])
        assert len(return_objs) == 3
