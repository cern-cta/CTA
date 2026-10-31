from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cta.sdk.daos.cli import FailedRequestDaoCli
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import Checksum, DiskFile, FailedRequest, FailedRequestSummary, MountType, TapeFile

Dao = tuple[FailedRequestDaoCli, MagicMock]


@pytest.fixture(scope="module")
def raw_data() -> str:
    return (Path(__file__).parent / "data" / "failed_requests.jsonl").read_text()


@pytest.fixture(scope="module")
def ref_obj() -> FailedRequest:
    return FailedRequest(
        id="RetrieveRequest-Frontend-ctapreproductionfrontend11.cern.ch-3560801-20250611-12:55:20-0-3",
        mount_type=MountType.RETRIEVE,
        requester_name="ctaops",
        requester_group="adm",
        tape_file=TapeFile(
            archive_id=4299888572,
            storage_class_name="",
            creation_time=datetime(2025, 6, 11, 14, 46, 11, tzinfo=timezone.utc),
            size=1024000,
            checksum=Checksum("UNKNOWN", "UNKNOWN"),
            disk_file=DiskFile(
                id="",
                disk_instance_name="",
                uid=-1,
                gid=-1,
                path="/eos/ctaatlaspps/ftstest/IBM460/a75bd88e-8c11-4652-a672-b6a268968343/0/test00000005",
            ),
            vid="I72509",
            fseq=1,
            block_id=0,
        ),
        tape_pool_name="",
        total_retries=0,
        logs="",
    )


class TestFailedRequestDaoCli:
    @pytest.fixture(autouse=True)
    def set_cli_wrapper(self, mock_wrapper: tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]]):
        self.manager = FailedRequestDaoCli(mock_wrapper[0])
        self.mock_run = mock_wrapper[1][0]
        self.mock_stream_run = mock_wrapper[1][1]

    def test_delete_command(self):
        self.manager.delete("RetrieveRequest-Frontend-ctapreproductionfrontend11.cern.ch-3560801-20250611-12:55:20-0-3")
        self.mock_run.assert_called_once_with(
            [
                "cta-admin",
                "--jsonl",
                "failedrequest",
                "rm",
                "--objectid",
                "RetrieveRequest-Frontend-ctapreproductionfrontend11.cern.ch-3560801-20250611-12:55:20-0-3",
            ]
        )

    @pytest.mark.parametrize(
        "mock_wrapper",
        [
            {
                "stdout": (
                    '{"requestType":"ARCHIVE_REQUEST","totalFiles":"0","totalSize":"0","schedule'
                    'rBackendName":"cephUser","instanceName":"preproduction"}\n{"requestType":"RE'
                    'TRIEVE_REQUEST","totalFiles":"51998","totalSize":"53241839472","schedulerBa'
                    'ckendName":"cephUser","instanceName":"preproduction"}\n{"requestType":"TOTAL'
                    '","totalFiles":"51998","totalSize":"53241839472","schedulerBackendName":"ce'
                    'phUser","instanceName":"preproduction"}\n'
                )
            }
        ],
        indirect=True,
    )
    def test_summary_command(self):
        return_obj = self.manager.summary()
        self.mock_run.assert_called_once_with(["cta-admin", "--jsonl", "failedrequest", "ls", "--summary"])
        assert asdict(return_obj[0]) == asdict(FailedRequestSummary(MountType.ARCHIVE, 0, 0, "cephUser"))

    @pytest.mark.parametrize("mock_wrapper", [{"stdout": slice(None)}], indirect=True)
    def test_list_command(self, ref_obj: FailedRequest):
        return_objs = list(self.manager.list(type=MountType.RETRIEVE, logs=True))
        self.mock_stream_run.assert_called_once_with(
            ["cta-admin", "--jsonl", "failedrequest", "ls", "--justretrieve", "--log"]
        )
        assert len(return_objs) == 3
        assert asdict(return_objs[0]) == asdict(ref_obj)
